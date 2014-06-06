-module(ens_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-compile(export_all).
%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ens.hrl").
-record(state, {node_info, process_info}).
-define(PREFIX, "ens_").
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
	[_, HostName] = string:tokens(atom_to_list(node()), "@"),
	MasterNode1 = list_to_atom("master1@" ++ HostName),
	MasterNode2 = list_to_atom("master2@" ++ HostName),
	{ok, _Ret} = mnesia:change_config(extra_db_nodes, [MasterNode1, MasterNode2]),
	create_process_table(),
	create_node_table(),
	NodeInfo = write_node_record(),
	register_to(MasterNode1),
	register_to(MasterNode2),
	{ok, #state{node_info=NodeInfo}}.

handle_call(_Msg, _From, State) ->
	{reply, ok, State}.

handle_cast({nodeup, Node}, State) ->
	monitor_node(Node, true),	
	{noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodedown, Node}, State) ->
	delete_node(Node),
	{noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
	#state{node_info=NodeInfo} = State,
	mnesia:dirty_delete_object(NodeInfo),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
create_node_table() ->
	create_table(ens_node, record_info(fields, ens_node)).

create_process_table() ->
	NodeType = application:get_env(ens, node_type, player),
	Table = table_name(NodeType),
	Info = [id, node, pid],
	create_table(Table, Info).		

create_table(Table, Info) ->
	case mnesia:add_table_copy(Table, node(), ram_copies) of
		{aborted, {no_exists, _}} ->
			mnesia:create_table(Table, [{attributes, Info}, {type, bag}]);
		{_, _} ->
			ok
	end.
	
write_node_record() ->
	NodeType = application:get_env(ens, node_type, master),
	NodeInfo = #ens_node{type=NodeType, node=node()},
	mnesia:dirty_write(NodeInfo),
	NodeInfo.	

get_node_type(Node) ->
	Table = ens_node,
	Record = {Table, '$1', Node},
	case mnesia:dirty_select(Table, [{Record, [], ['$1']}]) of
		[Type|_] ->
			{ok, Type};
		[] ->
			{error, not_found};
		{aborted, Reason} ->
			{error, Reason}
	end.	

get(Type, Id) ->
	Table = table_name(Type),
	Record = {Table, Id, '_', '$1'},
	case catch mnesia:dirty_select(Table, [{Record, [], ['$1']}]) of
		[Pid|_] ->
			{ok, Pid};
		[] ->
			{error, not_found};
		{aborted, Reason} ->
			{error, Reason}
	end.	

set(Type, Id, Node, Pid) ->
	Table = table_name(Type),
	ok = mnesia:dirty_write({Table, Id, Node, Pid}). 

delete(Type, Id) ->
	Table = table_name(Type),
	mnesia:dirty_delete(Table, Id).

delete_node(Node) ->
	case get_node_type(Node) of
		{ok, Type} ->
			do_delete_node(Type, Node);
		_ ->
			ok
	end.
	
do_delete_node(Type, Node) ->				
	mnesia:dirty_delete_object({ens_node, Type, Node}), 
	Table = table_name(Type),
	Record = {Table, '$1', Node, '_'},
	case catch mnesia:dirty_select(Table, [{Record, [], ['$1']}]) of
		[_] = Ids ->
			[mnesia:dirty_delete(Table, Id) || Id <- Ids];
		Reason ->
			{error, Reason}
	end.

create(Type) ->
	{_, Node} = get_node(Type),
	rpc:call(Node, poolboy, checkout, [pool]).

get_node(NodeType) ->
	Nodes = mnesia:read(ens_node, NodeType),
	N = random:uniform(length(Nodes)),
	lists:nth(N, Nodes).

register_to(Node) ->
	gen_server:cast({ens_server, Node}, {nodeup, node()}).

table_name(Type) ->
	list_to_atom(?PREFIX ++ atom_to_list(Type)).
