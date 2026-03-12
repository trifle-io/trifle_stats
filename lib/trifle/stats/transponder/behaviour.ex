defmodule Trifle.Stats.Transponder.Behaviour do
  @moduledoc """
  Behaviour for custom transponder implementations.

  The built-in transponder surface is expression-based. This behaviour remains
  available for custom transponder modules registered through the dynamic
  registry, where argument shape is defined by the generated fluent helper.
  """

  @callback transform(series :: map(), arg1 :: term(), arg2 :: term()) :: map()
  @callback transform(series :: map(), arg1 :: term(), arg2 :: term(), arg3 :: term()) :: map()
  @callback transform(
              series :: map(),
              arg1 :: term(),
              arg2 :: term(),
              arg3 :: term(),
              arg4 :: term()
            ) :: map()

  @optional_callbacks [transform: 3, transform: 4, transform: 5]
end
