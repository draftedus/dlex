defmodule Dlex.Config do
  @moduledoc """
  Holds state for configuration that calls need to access
  """
  use Agent

  @doc false
  def start_link(opts) do
    Agent.start_link(fn -> opts end, name: __MODULE__)
  end

  @doc false
  def get() do
    Agent.get(__MODULE__, & &1)
  end
end
