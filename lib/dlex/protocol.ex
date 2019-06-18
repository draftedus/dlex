defmodule Dlex.Protocol do
  @moduledoc false

  alias Dlex.{Error, Type, Query}
  alias Dlex.Api.TxnContext

  use DBConnection

  require Logger

  defstruct [:adapter, :channel, :connected, :opts, :txn_context, txn_aborted?: false]
  @request_options [:timeout]

  defp merge_options_for_request(opts, request_opts \\ []) do
    timeout = Keyword.get(request_opts, :timeout, Keyword.get(opts, :timeout, 15_000))
    [timeout: timeout]
  end

  @impl true
  def connect(opts) do
    Logger.debug(fn -> "Dlex.Protocol.connect(#{inspect(opts)})" end)
    host = opts[:hostname]
    port = opts[:port]
    adapter = opts[:adapter]

    case apply(adapter, :connect, ["#{host}:#{port}", opts]) do
      {:ok, channel} -> {:ok, %__MODULE__{adapter: adapter, channel: channel, opts: opts}}
      {:error, reason} -> {:error, %Error{action: :connect, reason: reason}}
    end
  end

  # Implement calls for DBConnection Protocol

  @impl true
  def ping(%{adapter: adapter, channel: channel} = state) do
    Logger.debug(fn -> "Dlex.Protocol.ping(#{inspect(state)})" end)

    case apply(adapter, :ping, [channel]) do
      :ok -> {:ok, state}
      {:disconnect, reason} -> {:disconnect, reason, state}
    end
  end

  @impl true
  def checkout(state) do
    Logger.debug(fn -> "Dlex.Protocol.checkout(#{inspect(state)})" end)
    {:ok, state}
  end

  @impl true
  def checkin(state) do
    Logger.debug(fn -> "Dlex.Protocol.checkin(#{inspect(state)})" end)
    {:ok, state}
  end

  @impl true
  def disconnect(error, %{adapter: adapter, channel: channel} = state) do
    Logger.debug(fn -> "Dlex.Protocol.disconnect(#{inspect(error)}, #{inspect(state)})" end)
    disconnect_stub(adapter, channel)
  end

  # disconnect from the grpc connection
  defp disconnect_stub(adapter, channel) do
    apply(adapter, :disconnect, [channel])
  end

  ## Transaction API

  @impl true
  def handle_begin(_opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_begin(#{inspect(state)})" end)
    {:ok, nil, %{state | txn_context: TxnContext.new(), txn_aborted?: false}}
  end

  @impl true
  def handle_rollback(_opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_rollback(#{inspect(state)})" end)
    finish_txn(state, :rollback)
  end

  @impl true
  def handle_commit(opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_commit(#{inspect(state)})" end)
    finish_txn(state, :commit, opts)
  end

  defp finish_txn(%{txn_aborted?: true} = state, txn_result) do
    {:error, %Error{action: txn_result, reason: :aborted}, state}
  end

  defp finish_txn(
         %{adapter: adapter, channel: channel, txn_context: txn_context, opts: opts} = state,
         txn_result,
         request_opts \\ []
       ) do
    state = %{state | txn_context: nil}
    merged_request_opts = merge_options_for_request(opts, request_opts)
    txn = %{txn_context | aborted: txn_result != :commit}

    case apply(adapter, :commit_or_abort, [channel, txn, merged_request_opts]) do
      {:ok, txn} -> {:ok, txn, state}
      {:error, error} -> {:error, %Error{action: txn_result, reason: error}, state}
    end
  end

  ## Query API

  @impl true
  def handle_prepare(query, opts, %{txn_context: txn_context} = state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_prepare(#{inspect(query)})" end)
    {:ok, %{query | txn_context: txn_context}, state}
  end

  @impl true
  def handle_execute(
        %Query{} = query,
        request,
        request_opts,
        %{channel: channel, opts: opts} = state
      ) do
    Logger.debug(fn -> "Dlex.Protocol.handle_execute(#{inspect(query)})" end)

    merged_request_opts = merge_options_for_request(opts, request_opts) ++ [adapter: state.adapter]

    case Type.execute(channel, query, request, merged_request_opts) do
      {:ok, result} ->
        {:ok, query, result, check_txn(state, result)}

      {:error, error} ->
        {:error, %Error{action: :execute, reason: error}, check_abort(state, error)}
    end
  end

  defp check_txn(state, result) do
    case result do
      %{txn: %TxnContext{} = txn_context} -> merge_txn(state, txn_context)
      %{context: %TxnContext{} = txn_context} -> merge_txn(state, txn_context)
      _ -> state
    end
  end

  defp check_abort(state, %GRPC.RPCError{status: 10}), do: %{state | txn_aborted?: true}
  defp check_abort(state, _error), do: state

  defp merge_txn(%{txn_context: nil} = state, _), do: state

  defp merge_txn(%{txn_context: %TxnContext{} = txn_context} = state, new_txn_context) do
    %{start_ts: start_ts, keys: keys, preds: preds} = txn_context
    %{start_ts: new_start_ts, keys: new_keys, preds: new_preds} = new_txn_context
    start_ts = if start_ts == 0, do: new_start_ts, else: start_ts
    keys = keys ++ new_keys
    preds = preds ++ new_preds
    %{state | txn_context: %{txn_context | start_ts: start_ts, keys: keys, preds: preds}}
  end

  @impl true
  def handle_close(query, _opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_close(#{inspect(query)}, #{inspect(state)})" end)
    {:ok, nil, state}
  end

  @impl true
  def handle_status(_opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_status(#{inspect(state)})" end)
    {:idle, state}
  end

  ## Stream API

  @impl true
  def handle_declare(query, _params, _opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_declare(#{inspect(query)}, #{inspect(state)})" end)
    {:ok, query, nil, state}
  end

  @impl true
  def handle_fetch(query, _cursor, _opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_fetch(#{inspect(query)}, #{inspect(state)})" end)
    {:halt, nil, state}
  end

  @impl true
  def handle_deallocate(query, _cursor, _opts, state) do
    Logger.debug(fn -> "Dlex.Protocol.handle_deallocate(#{inspect(query)}, #{inspect(state)})" end)

    {:ok, nil, state}
  end
end
