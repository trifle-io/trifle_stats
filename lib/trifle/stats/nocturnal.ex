defmodule Trifle.Stats.Nocturnal do
  alias Trifle.Stats.Nocturnal.Key
  
  def days_into_week, do: %{monday: 1, tuesday: 2, wednesday: 3, thursday: 4, friday: 5, saturday: 6, sunday: 7}
  
  @doc """
  Creates a Key object for a specific time granularity.
  
  ## Examples
  
      config = Trifle.Stats.Configuration.configure(nil)
      key = Trifle.Stats.Nocturnal.key_for("page_views", :hour, ~U[2025-08-17 10:30:00Z], config)
      # Creates key with proper time boundary for the hour
  """
  def key_for(key_name, granularity, at, config) do
    {:ok, boundary_time} = apply_granularity(at, granularity, config)
    Key.new(key: key_name, granularity: to_string(granularity), at: boundary_time)
  end

  def timeline(from, to, granularity, config) do
    {:ok, from} = apply(Trifle.Stats.Nocturnal, granularity, [from, config])
    {:ok, to} = apply(Trifle.Stats.Nocturnal, granularity, [to, config])

    timeline = Stream.unfold(from, fn at ->
      {:ok, next} = next(at, granularity, config)

      case DateTime.compare(next, to) do
        :lt -> {next, next}
        :eq -> {next, next}
        _ -> nil
      end
    end) |> Enum.to_list()

    [from] ++ timeline
  end

  def next(at, granularity, config) do
    {:ok, _at} = apply(Trifle.Stats.Nocturnal, :"next_#{granularity}", [at, config])
  end

  def apply_granularity(at, granularity, config) do
    apply(Trifle.Stats.Nocturnal, granularity, [at, config])
  end

  def change(at, config, year \\ nil, month \\ nil, day \\ nil, hour \\ nil, minute \\ nil, second \\ 0) do
    with {:ok, date} <- Date.new(year || at.year, month || at.month, day || at.day),
         {:ok, time} <- Time.new(hour || at.hour, minute || at.minute, second || at.second)
    do
      DateTime.new(date, time, config.time_zone, config.time_zone_database)
    else
      err -> err
    end
  end

  def second(at, config) do
    # For second boundaries, we want to keep the exact second (truncate microseconds)
    change(at, config, nil, nil, nil, nil, nil, at.second)
  end

  def next_second(at, config) do
    Trifle.Stats.Nocturnal.second(
      DateTime.add(at, 1, :second, config.time_zone_database), config
    )
  end

  def minute(at, config) do
    change(at, config, nil, nil, nil, nil, nil, 0)
  end

  def next_minute(at, config) do
    Trifle.Stats.Nocturnal.minute(
      DateTime.add(at, 1, :minute, config.time_zone_database), config
    )
  end

  def hour(at, config) do
    change(at, config, nil, nil, nil, nil, 0)
  end

  def next_hour(at, config) do
    Trifle.Stats.Nocturnal.hour(
      DateTime.add(at, 1, :hour, config.time_zone_database), config
    )
  end

  def day(at, config) do
    change(at, config, nil, nil, nil, 0, 0)
  end

  def next_day(at, config) do
    Trifle.Stats.Nocturnal.day(
      DateTime.add(at, 1, :day, config.time_zone_database), config
    )
  end

  def week(at, config) do
    at = DateTime.add(
      at, -days_to_week_start(at, config), :day, config.time_zone_database
    )
    change(at, config, nil, nil, nil, 0, 0)
  end

  def next_week(at, config) do
    Trifle.Stats.Nocturnal.week(
      DateTime.add(at, 7, :day, config.time_zone_database), config
    )
  end

  def days_to_week_start(at, config) do
    beginning_of_week = days_into_week()[config.beginning_of_week]
    current_day = Date.day_of_week(at)
    
    # Calculate days from current day to beginning of week
    days = rem(current_day - beginning_of_week + 7, 7)
    days
  end

  def month(at, config) do
    change(at, config, nil, nil, 1, 0, 0)
  end

  def next_month(at, config) do
    Trifle.Stats.Nocturnal.month(
      DateTime.add(at, 31, :day, config.time_zone_database), config
    )
  end

  def quarter(at, config) do
    first_quarter_month = at.month - rem((2 + at.month), 3)
    change(at, config, nil, first_quarter_month, 1, 0, 0)
  end

  def next_quarter(at, config) do
    Trifle.Stats.Nocturnal.quarter(
      DateTime.add(at, 31 * 3, :day, config.time_zone_database), config
    )
  end

  def year(at, config) do
    change(at, config, nil, 1, 1, 0, 0)
  end

  def next_year(at, config) do
    Trifle.Stats.Nocturnal.year(
      DateTime.add(at, 366, :day, config.time_zone_database), config
    )
  end
end
