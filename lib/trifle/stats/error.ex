defmodule Trifle.Stats.Error do
  @moduledoc """
  Base error module for Trifle.Stats.
  """
  defexception [:message]

  @impl true
  def exception(value) do
    %__MODULE__{message: "#{value}"}
  end
end

defmodule Trifle.Stats.DriverNotFoundError do
  @moduledoc """
  Error raised when a required driver is not found or not configured.
  """
  defexception [:message]

  @impl true
  def exception(value) do
    %__MODULE__{message: "Driver not found: #{value}"}
  end
end

defmodule Trifle.Stats.ConfigurationError do
  @moduledoc """
  Error raised when configuration is invalid.
  """
  defexception [:message]

  @impl true
  def exception(value) do
    %__MODULE__{message: "Configuration error: #{value}"}
  end
end

defmodule Trifle.Stats.ValidationError do
  @moduledoc """
  Error raised when input validation fails.
  """
  defexception [:message]

  @impl true
  def exception(value) do
    %__MODULE__{message: "Validation error: #{value}"}
  end
end