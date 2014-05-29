-module(ens_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

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

-define(NODE_INFO, record_info(fields, ens_node)).
-define(PLAYER_INFO, record_info(fields, ens_player)).
-define(MAP_INFO, record_info(fields, ens_map)).
-define(UNION_INFO, record_info(fields, ens_union)).

-define(PLAYER(Id), #ens_player{id = Id, pid = '$1'}).
-define(MAP(Id), #ens_map{id = Id, pid = '$1'}).
-define(UNION(Id), #ens_player{id = Id, pid = '$1'}).
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
	{ok, #state{node_info=NodeInfo}}.

handle_call({get_node, NodeType}, _From, State) ->
	{reply, get_node(NodeType), State};

handle_call({get_player_pid, PlayerId}, _, State) ->
	Reply = find_pid(player, PlayerId),
	{reply, Reply, State};

handle_call({get_map_pid, MapId}, _, State) ->
	Reply = find_pid(map, MapId),
	{reply, Reply, State};

handle_call({get_union_pid, UnionId}, _, State) ->
	Reply = find_pid(union, UnionId),
	{reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

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
	create_table(ens_nodes, ?NODE_INFO).
	
write_node_record() ->
	{ok,  NodeType} = application:get_env(node_type),
	NodeInfo = #ens_node{type=NodeType, node=node()},
	mnesia:dirty_write(NodeInfo),
	NodeInfo.

create_pid(Type, Id) ->
	case Type of
		player ->
			create(ens_players, ?PLAYER(Id));
		map ->
			create(ens_maps, ?MAP(Id));
		union ->
			create(ens_union, ?UNION(Id))
	end.

create(Type, Id) ->
	{_, Node} = get_node(Type),
	Pid = rpc:call(Node, poolboy, checkout, [map_pool]),
	Pid!{set_id, Id, Pid}.

find_pid(Type, Id) ->
	case Type of
		player ->
			find(ens_players, ?PLAYER(Id));
		map ->
			find(ens_maps, ?MAP(Id));
		union ->
			find(ens_union, ?UNION(Id))
	end.

find(Table, Record) ->
	case mnesia:dirty_select(Table, [{Record, [], ['$1']}]) of
		[Pid] ->
			{ok, Pid};
		[] ->
			{error, not_found};
		{aborted, Reason} ->
			{error, Reason}
	end.		

create_process_table() ->
	{ok,  NodeType} = application:get_env(node_type),
	case NodeType of
		player ->
			create_table(ens_players, ?PLAYER_INFO);
		map ->
			create_table(ens_maps, ?MAP_INFO);
		union ->
			create_table(ens_union, ?UNION_INFO)
	end.							

create_table(Table, Info) ->
	case mnesia:add_table_copy(Table, node(), ram_copies) of
		{aborted, {no_exists, _}} ->
			mnesia:create_table(Table, [{attributes, Info}, {type, bag}]);
		{_, _} ->
			ok
	end.

get_node(NodeType) ->
	Nodes = mnesia:read(ens_nodes, NodeType),
	N = random:uniform(length(Nodes)),
	lists:nth(N, Nodes).

