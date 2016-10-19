%%%-------------------------------------------------------------------
%%% @author yinqw
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. 十月 2016 下午2:21
%%%-------------------------------------------------------------------
-module(erldb_otp).
-author("yinqw").

-behaviour(gen_server).
%% API
-export([start_link/1]).
-compile(export_all).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(default_connect_timeout, 5000).
-define(default_tns, <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=orcl)))">>).
-define(Timeout, 5000).

-record(state, {db, conn, stmts=dict:new()}).
-record(prepared, {query, type}).
-record(mysql, {pid}).
-record(oracle, {oci_port=undefined, oci_session, tns, user, password}).

select(Conn, StatementRef, Params) ->
    gen_server:call(Conn, {select, StatementRef, Params}).
execute(Conn, StatementRef, Params) ->
    gen_server:call(Conn, {execute, StatementRef, Params}).

start_link(Options) ->
    Ret = gen_server:start_link(?MODULE, Options, [{timeout, ?default_connect_timeout}]),
    case Ret of
        {ok, Pid} ->
            Db = proplists:get_value(db, Options, []),
            Config = proplists:get_value(config, Options, []),
            case Db of
                mysql ->
                    ok;
                _ ->
                    Prepare = proplists:get_value(prepare, Config, []),
                    lists:foreach(fun ({Name, Query, ParaType}) ->
                                          Stmt = #prepared{query = Query, type = ParaType},
                                          prepare(Pid, Name, Stmt)
                                  end, Prepare)
            end;
        _ -> ok
    end,
    Ret.

prepare(Pid, Name, Stmt) ->
    gen_server:call(Pid, {prepare, Name, Stmt}).

init([{db, mysql}, {config, Config}]) ->
    {ok, Pid} = mysql:start_link(Config),
    State = #state{db=mysql, conn = #mysql{pid = Pid}},
    {ok, State};
init([_, {config, Config}]) ->
    OciPortConf    = proplists:get_value(oci_port, Config),
    Tns            = to_binary(proplists:get_value(tns, Config, ?default_tns)),
    User           = to_binary(proplists:get_value(user, Config)),
    Password       = to_binary(proplists:get_value(password, Config)),
    OciPort = erloci:new(OciPortConf),
    OciSession = OciPort:get_session(Tns, User, Password),
    State = #state{db = oracle,
                   conn =
                       #oracle{
                          oci_port = OciPort,
                          oci_session = OciSession,
                          tns = Tns,
                          user = User,
                          password = Password}},
    {ok, State}.

handle_call({prepare, Name, Stmt}, _From, State) when is_atom(Name) ->
    Stmts1 = dict:store(Name, Stmt, State#state.stmts),
    State2 = State#state{stmts = Stmts1},
    {reply, ok, State2};
handle_call({execute, Stmt, Args}, _From, #state{
                                             db = mysql,
                                             conn = #mysql{pid = Pid}} = State) ->
    Result = mysql:execute(Pid, Stmt, Args, ?Timeout),
    {reply, Result, State};
handle_call({execute, Stmt, Args}, _From, #state{db = oracle} = State) ->
    case dict:find(Stmt, State#state.stmts) of
        {ok, StmtRec} ->
            execute_stmt(StmtRec, Args, State);
        error ->
            {reply, {error, not_prepared}, State}
    end;
handle_call({select, Stmt, Args}, _From, #state{
                                            db = mysql,
                                            conn = #mysql{pid = Pid}} = State) ->
    Result = mysql:execute(Pid, Stmt, Args, ?Timeout),
    {reply, Result, State};
handle_call({select, Stmt, Args}, _From, #state{db = oracle} = State) ->
    case dict:find(Stmt, State#state.stmts) of
        {ok, StmtRec} ->
            execute_stmt(StmtRec, Args, State, select);
        error ->
            {reply, {error, not_prepared}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
execute_stmt(#prepared{query = Query, type = Type}, Args,
             State=#state{conn =Orcl}) ->
    OciSession = get_session(Orcl),
    Stmt = OciSession:prep_sql(Query),
    Stmt:bind_vars(Type),
    StmtResult = Stmt:exec_stmt(Args),
    Result = handle_query_call_reply({execute, StmtResult}),
    {reply, Result, State}.
execute_stmt(#prepared{query = Query, type = []}, _Args,
             State=#state{conn =Orcl}, select) ->
    OciSession = get_session(Orcl),
    SelStmt = OciSession:prep_sql(Query),
    Cols = SelStmt:exec_stmt(),
    Result = handle_select_reply(SelStmt, Cols),
    {reply, Result, State};
execute_stmt(#prepared{query = Query, type = Type}, Args,
             State=#state{conn =Orcl}, select) ->
    OciSession = get_session(Orcl),
    SelStmt = OciSession:prep_sql(Query),
    SelStmt:bind_vars(Type),
    Cols = SelStmt:exec_stmt(Args),
    Result = handle_select_reply(SelStmt, Cols),
    {reply, Result, State}.

to_binary(P) when is_list(P) ->
    list_to_binary(P);
to_binary(P) when is_binary(P)->
    P.

handle_query_call_reply({execute, Result}) ->
    %%  {executed,0,[{<<":out_id">>,10}]}
    %%  {rowids,[<<"AAAickAANAAAEFmAAC">>]}
    Result.
handle_select_reply(SelStmt, {cols, SelType}) ->
    handle_select_reply(SelStmt, SelType, [], false).
handle_select_reply(_SelStmt, _SelType, ValList, true) ->
    lists:reverse(ValList);
handle_select_reply(SelStmt, SelType, ValList, false) ->
    {{rows,Value}, Flag} = SelStmt:fetch_rows(1),
    Final =
        case Value of
            [] ->
                [];
            [Value1] ->
                list_to_tuple(lists:zipwith(fun({_,X,_,_,_}, Y) ->
                                                    case X of
                                                        'SQLT_TIMESTAMP' ->
                                                            {Data, Time, _} = oci_util:from_dts(Y),
                                                            {Data, Time};
                                                        'SQLT_CHR' ->
                                                            Y;
                                                        'SQLT_NUM' ->
                                                            oci_util:from_num(Y);
                                                        'SQLT_INT' ->
                                                            oci_util:from_num(Y)
                                                    end
                                            end, SelType, Value1))
        end,
    handle_select_reply(SelStmt, SelType, [Final|ValList], Flag).

get_session(#oracle{oci_port = OciPort, oci_session = OciSession, tns=Tns, user = User, password = Password}) ->
    try
        ok = OciSession:ping(),
        OciSession
    catch
        _Class:_Error  ->
            lager:warning("Oracle Session disconnect, create a new session..."),
            OciPort:get_session(Tns, User, Password)
    end.

%%==============================================================================
test() ->
    Opt = [{db, oracle}, {config, [{oci_port,[{logging, false},{env, [{"NLS_LANG", "AMERICAN_AMERICA.UTF8"}]}]},
                                   {tns,"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=101.200.195.107)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=orcl)))"},
                                   {user, "euroideauser"},
                                   {password, "euroidea!29)"},
                                   {prepare, [
                                              {'erl_test.select', "select * from ERL_TEST where id = :id", [{<<":id">>, 'SQLT_INT'}]},
                                              {'erl_test.select2', "select * from ERL_TEST", []},
                                              {'erl_test.insert', "insert into ERL_TEST(test1, now, num) values(:test1,:now, :num)",
                                               [{<<":test1">>, 'SQLT_CHR'}, {<<":now">>, 'SQLT_TIMESTAMP'}, {<<":num">>, 'SQLT_INT'}]},
                                              {'erl_test.update', "update ERL_TEST set test1=:test1", [{<<":test1">>, 'SQLT_CHR'}]},
                                              {'erl_test.proc', "call PROC_ERL_TEST(:test1,:now, :num, :out_id)",
                                               [{<<":test1">>, in, 'SQLT_CHR'}, {<<":now">>, in, 'SQLT_TIMESTAMP'}, {<<":num">>, 'SQLT_INT'}, {<<":out_id">>, out, 'SQLT_INT'}]}
                                             ]
                                   }
                                  ]}],
    Opt1 = [{db, mysql}, {config,[{host,"192.168.1.63"},{user, "mycomm"}, {password, "mycomm123"}, {database, "c8config"},
                                  {prepare,
                                   [
                                    {'erl_test.insert', "insert into erl_test(test1) value(?)"}
                                   ]
                                  }
                                 ]}],
    {ok, Pid} = erldb_otp:start_link(Opt1),
    Time = oci_util:edatetime_to_ora(os:timestamp()),
    %%  R = execute_select(Pid, 'erl_test.select2', []),
    %%  R = execute(Pid, 'erl_test.insert', [{<<"搞笑"/utf8>>, oci_util:edatetime_to_ora(os:timestamp()), 12}]),
    %%  R = execute(Pid, 'erl_test.update', [{<<"搞笑"/utf8>>}]),
    %%    R = execute(Pid, 'erl_test.proc', [{<<"搞笑"/utf8>>, Time, 13, 1}]),
    R = execute(Pid, 'erl_test.insert', [<<"abcde">>]),
    io:format("rrrrrrrrrrrrr:~p~n", [R]).