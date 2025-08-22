defmodule Trifle.Stats.Nocturnal do

  @unit_map %{
    "s" => :second,
    "m" => :minute,
    "h" => :hour,
    "d" => :day,
    "w" => :week,
    "mo" => :month,
    "q" => :quarter,
    "y" => :year
  }

  def days_into_week, do: %{monday: 1, tuesday: 2, wednesday: 3, thursday: 4, friday: 5, saturday: 6, sunday: 7}

  @spec timeline([
          {:config, any()}
          | {:from, any()}
          | {:offset, pos_integer()}
          | {:to, any()}
          | {:unit, :day | :hour | :minute | :month | :quarter | :second | :week | :year},
          ...
        ]) :: list()
  @doc """
  Create timeline using string-based granularity.

  ## Examples

      config = Trifle.Stats.Configuration.configure(nil)
      timeline = Trifle.Stats.Nocturnal.timeline(
        from: ~U[2025-08-17 10:00:00Z],
        to: ~U[2025-08-17 12:00:00Z],
        offset: 15,
        unit: :minute,
        config: config
      )

  """
  def timeline(from: from, to: to, offset: offset, unit: unit, config: config) do
    floored_from = new(from, config) |> floor(offset, unit)
    floored_to = new(to, config) |> floor(offset, unit)

    Stream.unfold(floored_from, fn current ->
      if DateTime.compare(current, floored_to) == :gt do
        nil
      else
        next_time = new(current, config) |> add(offset, unit)
        {current, next_time}
      end
    end)
    |> Enum.to_list()
  end

  defstruct [:time, :config]

  @doc """
  Create a new Nocturnal instance with time and config.

  ## Examples

      nocturnal = Trifle.Stats.Nocturnal.new(~U[2025-08-17 10:30:00Z], config)

  """
  def new(time, config) do
    %__MODULE__{time: time, config: config}
  end


  @doc """
  Add time offset to the current time based on unit.
  Supports both positive (forward) and negative (backward) offsets.

  ## Examples

      nocturnal = Trifle.Stats.Nocturnal.new(~U[2025-08-17 10:30:00Z], config)
      new_time = Trifle.Stats.Nocturnal.add(nocturnal, 15, :minute)
      # Returns time 15 minutes later

      past_time = Trifle.Stats.Nocturnal.add(nocturnal, -24, :hour)
      # Returns time 24 hours earlier

  """
  def add(%__MODULE__{time: time, config: config}, offset, unit) do
    unless is_struct(time, DateTime) do
      raise ArgumentError, "Expected DateTime object, got #{inspect(time.__struct__)}"
    end

    unless is_integer(offset) do
      raise ArgumentError, "Offset must be an integer"
    end

    unless Map.values(@unit_map) |> Enum.member?(unit) do
      raise ArgumentError, "Invalid unit: #{unit}"
    end

    case unit do
      :second ->
        DateTime.add(time, offset, :second, config.time_zone_database || Tzdata.TimeZoneDatabase)

      :minute ->
        DateTime.add(time, offset * 60, :second, config.time_zone_database || Tzdata.TimeZoneDatabase)

      :hour ->
        DateTime.add(time, offset * 3600, :second, config.time_zone_database || Tzdata.TimeZoneDatabase)

      :day ->
        DateTime.add(time, offset, :day, config.time_zone_database || Tzdata.TimeZoneDatabase)

      :week ->
        DateTime.add(time, offset * 7, :day, config.time_zone_database || Tzdata.TimeZoneDatabase)

      :month ->
        add_months(time, offset, config)

      :quarter ->
        add_months(time, offset * 3, config)

      :year ->
        add_years(time, offset, config)
    end
  end

  @doc """
  Floor time to the segment boundary based on offset and unit.
  Handles edge cases like 33-minute segments that create uneven boundaries.

  ## Examples

      nocturnal = Trifle.Stats.Nocturnal.new(~U[2025-08-17 10:37:45Z], config)
      floored = nocturnal.floor(15, :minute)
      # Returns ~U[2025-08-17 10:30:00Z] (start of 15-minute segment)

      # Edge case with 33-minute segments
      floored = nocturnal.floor(33, :minute)
      # For 10:37:45, returns ~U[2025-08-17 10:33:00Z] (33-59 segment)

  """
  def floor(%__MODULE__{time: time, config: config}, offset, unit) do
    unless is_struct(time, DateTime) do
      raise ArgumentError, "Expected DateTime object, got #{inspect(time.__struct__)}"
    end

    unless is_integer(offset) and offset > 0 do
      raise ArgumentError, "Segment size must be positive"
    end

    unless Map.values(@unit_map) |> Enum.member?(unit) do
      raise ArgumentError, "Invalid unit: #{unit}"
    end

    tz_database = config.time_zone_database || Tzdata.TimeZoneDatabase

    case unit do
      :second ->
        total_seconds = time.second
        floored_seconds = div(total_seconds, offset) * offset
        %{time | second: floored_seconds, microsecond: {0, 6}}

      :minute ->
        # Floor to minute segment boundary (segments start from beginning of hour)
        minutes_from_hour_start = time.minute
        floored_minutes = div(minutes_from_hour_start, offset) * offset
        %{time | minute: floored_minutes, second: 0, microsecond: {0, 6}}

      :hour ->
        # Floor to hour segment boundary (segments start from beginning of day)
        hours_from_day_start = time.hour
        floored_hours = div(hours_from_day_start, offset) * offset
        %{time | hour: floored_hours, minute: 0, second: 0, microsecond: {0, 6}}

      :day ->
        # Floor to day segment boundary (segments start from beginning of year)
        day_of_year = Date.day_of_year(DateTime.to_date(time))
        days_from_year_start = day_of_year - 1  # Convert to 0-indexed
        floored_days = div(days_from_year_start, offset) * offset

        year_start = %{time | month: 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 6}}
        DateTime.add(year_start, floored_days, :day, tz_database)

      :week ->
        # Floor to week segment boundary (segments start from beginning of year)
        year_start = %{time | month: 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 6}}

        # Find the first week boundary of the year based on week_start
        week_start_offset = Map.get(days_into_week(), config.beginning_of_week, 1)
        year_start_wday = Date.day_of_week(DateTime.to_date(year_start))
        days_to_first_week_start = rem(week_start_offset - year_start_wday + 7, 7)
        first_week_start = DateTime.add(year_start, days_to_first_week_start, :day, tz_database)

        # If current time is before first week boundary, use year start
        if DateTime.compare(time, first_week_start) == :lt do
          year_start
        else
          # Calculate weeks since first week start
          diff_seconds = DateTime.diff(time, first_week_start, :second)
          weeks_since_first = div(diff_seconds, 7 * 86_400)
          floored_weeks = div(weeks_since_first, offset) * offset

          DateTime.add(first_week_start, floored_weeks * 7, :day, tz_database)
        end

      :month ->
        # Floor to month segment boundary (from start of year)
        months_from_jan = time.month - 1  # 0-indexed
        floored_months = div(months_from_jan, offset) * offset
        %{time | month: floored_months + 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 6}}

      :quarter ->
        # Floor to quarter segment boundary
        current_quarter = div(time.month - 1, 3)  # 0-indexed quarters
        floored_quarters = div(current_quarter, offset) * offset
        quarter_start_month = floored_quarters * 3 + 1
        %{time | month: quarter_start_month, day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 6}}

      :year ->
        # Floor to year segment boundary
        floored_years = div(time.year, offset) * offset
        %{time | year: floored_years, month: 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 6}}
    end
  end

  # Helper function to add months, handling edge cases
  defp add_months(time, months_to_add, config) do
    tz_database = config.time_zone_database || Tzdata.TimeZoneDatabase

    {new_year, new_month} = add_months_to_date(time.year, time.month, months_to_add)
    max_day = days_in_month(new_year, new_month)
    new_day = min(time.day, max_day)

    case DateTime.new(
      Date.new!(new_year, new_month, new_day),
      Time.new!(time.hour, time.minute, time.second, time.microsecond),
      time.time_zone,
      tz_database
    ) do
      {:ok, new_datetime} -> new_datetime
      {:error, reason} -> raise ArgumentError, "Error adding months: #{inspect(reason)}"
    end
  end

  # Helper function to add years, handling leap year edge cases
  defp add_years(time, years_to_add, config) do
    tz_database = config.time_zone_database || Tzdata.TimeZoneDatabase
    new_year = time.year + years_to_add

    # Handle leap year edge case (Feb 29)
    {final_year, final_month, final_day} =
      if time.month == 2 and time.day == 29 and not Date.leap_year?(%Date{year: new_year, month: 1, day: 1}) do
        {new_year, 2, 28}  # Feb 29 -> Feb 28 in non-leap year
      else
        {new_year, time.month, time.day}
      end

    case DateTime.new(
      Date.new!(final_year, final_month, final_day),
      Time.new!(time.hour, time.minute, time.second, time.microsecond),
      time.time_zone,
      tz_database
    ) do
      {:ok, new_datetime} -> new_datetime
      {:error, reason} -> raise ArgumentError, "Error adding years: #{inspect(reason)}"
    end
  end

  # Helper function to calculate year/month after adding months
  defp add_months_to_date(year, month, months_to_add) do
    total_months = month + months_to_add - 1  # Convert to 0-indexed
    new_year = year + div(total_months, 12)
    new_month = rem(total_months, 12) + 1  # Convert back to 1-indexed
    {new_year, new_month}
  end

  # Helper function to get days in a month
  defp days_in_month(year, month) do
    Date.days_in_month(Date.new!(year, month, 1))
  end
end
