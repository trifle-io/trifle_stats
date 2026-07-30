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

  def days_into_week,
    do: %{monday: 1, tuesday: 2, wednesday: 3, thursday: 4, friday: 5, saturday: 6, sunday: 7}

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
        next_time =
          current
          |> new(config)
          |> add(offset, unit)
          |> new(config)
          |> floor(offset, unit)

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
    normalized_time =
      if is_struct(time, DateTime) do
        DateTime.shift_zone!(
          time,
          config.time_zone || "Etc/UTC",
          config.time_zone_database || Tzdata.TimeZoneDatabase
        )
      else
        time
      end

    %__MODULE__{time: normalized_time, config: config}
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
        DateTime.add(
          time,
          offset * 60,
          :second,
          config.time_zone_database || Tzdata.TimeZoneDatabase
        )

      :hour ->
        DateTime.add(
          time,
          offset * 3600,
          :second,
          config.time_zone_database || Tzdata.TimeZoneDatabase
        )

      :day ->
        add_calendar_days(time, offset, config)

      :week ->
        add_calendar_days(time, offset * 7, config)

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
        microseconds = (total_seconds - floored_seconds) * 1_000_000 + elem(time.microsecond, 0)
        DateTime.add(time, -microseconds, :microsecond, tz_database)

      :minute ->
        # Floor to minute segment boundary (segments start from beginning of hour)
        minutes_from_hour_start = time.minute
        floored_minutes = div(minutes_from_hour_start, offset) * offset

        microseconds =
          ((minutes_from_hour_start - floored_minutes) * 60 + time.second) * 1_000_000 +
            elem(time.microsecond, 0)

        DateTime.add(time, -microseconds, :microsecond, tz_database)

      :hour ->
        # Hour segments are elapsed durations anchored at local day start.
        day_start = local_midnight(DateTime.to_date(time), time, tz_database)
        segment = offset * 3_600_000_000
        elapsed = DateTime.diff(time, day_start, :microsecond)
        DateTime.add(day_start, div(elapsed, segment) * segment, :microsecond, tz_database)

      :day ->
        # Floor to day segment boundary (segments start from beginning of year)
        day_of_year = Date.day_of_year(DateTime.to_date(time))
        # Convert to 0-indexed
        days_from_year_start = day_of_year - 1
        floored_days = div(days_from_year_start, offset) * offset

        result_date = Date.add(Date.new!(time.year, 1, 1), floored_days)
        local_midnight(result_date, time, tz_database)

      :week ->
        # Floor to week segment boundary (segments start from beginning of year)
        current_date = DateTime.to_date(time)
        year_start = Date.new!(time.year, 1, 1)

        # Find the first week boundary of the year based on week_start
        week_start_offset = Map.get(days_into_week(), config.beginning_of_week, 1)
        year_start_wday = Date.day_of_week(year_start)
        days_to_first_week_start = rem(week_start_offset - year_start_wday + 7, 7)
        first_week_start = Date.add(year_start, days_to_first_week_start)

        # If current time is before first week boundary, use year start
        if Date.compare(current_date, first_week_start) == :lt do
          local_midnight(year_start, time, tz_database)
        else
          weeks_since_first = div(Date.diff(current_date, first_week_start), 7)
          floored_weeks = div(weeks_since_first, offset) * offset

          first_week_start
          |> Date.add(floored_weeks * 7)
          |> local_midnight(time, tz_database)
        end

      :month ->
        # Floor to month segment boundary (from start of year)
        # 0-indexed
        months_from_jan = time.month - 1
        floored_months = div(months_from_jan, offset) * offset

        Date.new!(time.year, floored_months + 1, 1)
        |> local_midnight(time, tz_database)

      :quarter ->
        # Floor to quarter segment boundary
        # 0-indexed quarters
        current_quarter = div(time.month - 1, 3)
        floored_quarters = div(current_quarter, offset) * offset
        quarter_start_month = floored_quarters * 3 + 1

        Date.new!(time.year, quarter_start_month, 1)
        |> local_midnight(time, tz_database)

      :year ->
        # Floor to year segment boundary
        floored_years = div(time.year, offset) * offset

        Date.new!(floored_years, 1, 1)
        |> local_midnight(time, tz_database)
    end
  end

  defp add_calendar_days(time, days, config) do
    tz_database = config.time_zone_database || Tzdata.TimeZoneDatabase
    date = time |> DateTime.to_date() |> Date.add(days)
    local_datetime(date, DateTime.to_time(time), time, tz_database)
  end

  # Helper function to add months, handling edge cases
  defp add_months(time, months_to_add, config) do
    tz_database = config.time_zone_database || Tzdata.TimeZoneDatabase

    {new_year, new_month} = add_months_to_date(time.year, time.month, months_to_add)
    max_day = days_in_month(new_year, new_month)
    new_day = min(time.day, max_day)

    local_datetime(
      Date.new!(new_year, new_month, new_day),
      Time.new!(time.hour, time.minute, time.second, time.microsecond),
      time,
      tz_database
    )
  end

  # Helper function to add years, handling leap year edge cases
  defp add_years(time, years_to_add, config) do
    tz_database = config.time_zone_database || Tzdata.TimeZoneDatabase
    new_year = time.year + years_to_add

    # Handle leap year edge case (Feb 29)
    {final_year, final_month, final_day} =
      if time.month == 2 and time.day == 29 and
           not Date.leap_year?(%Date{year: new_year, month: 1, day: 1}) do
        # Feb 29 -> Feb 28 in non-leap year
        {new_year, 2, 28}
      else
        {new_year, time.month, time.day}
      end

    local_datetime(
      Date.new!(final_year, final_month, final_day),
      Time.new!(time.hour, time.minute, time.second, time.microsecond),
      time,
      tz_database
    )
  end

  # Helper function to calculate year/month after adding months
  defp add_months_to_date(year, month, months_to_add) do
    # Convert to 0-indexed
    total_months = month + months_to_add - 1
    year_delta = div(total_months, 12)
    month_index = rem(total_months, 12)

    if month_index < 0 do
      {year + year_delta - 1, month_index + 13}
    else
      {year + year_delta, month_index + 1}
    end
  end

  defp local_midnight(date, source, tz_database) do
    local_datetime(date, ~T[00:00:00], source, tz_database)
  end

  defp local_datetime(date, wall_time, source, tz_database) do
    case DateTime.new(date, wall_time, source.time_zone, tz_database) do
      {:ok, datetime} ->
        datetime

      {:ambiguous, first, second} ->
        source_offset = source.utc_offset + source.std_offset

        Enum.find(
          [first, second],
          first,
          fn candidate -> candidate.utc_offset + candidate.std_offset == source_offset end
        )

      {:gap, before_gap, after_gap} ->
        gap = transition_gap(before_gap, after_gap)
        shifted = date |> NaiveDateTime.new!(wall_time) |> NaiveDateTime.add(gap, :microsecond)
        shifted_time = NaiveDateTime.to_time(shifted)

        shifted_time = %{
          shifted_time
          | microsecond: {elem(shifted_time.microsecond, 0), elem(wall_time.microsecond, 1)}
        }

        local_datetime(NaiveDateTime.to_date(shifted), shifted_time, source, tz_database)

      {:error, reason} ->
        raise ArgumentError, "Unable to resolve local time: #{inspect(reason)}"
    end
  end

  defp transition_gap(before_gap, after_gap) do
    NaiveDateTime.diff(
      DateTime.to_naive(after_gap),
      DateTime.to_naive(before_gap),
      :microsecond
    ) - 1
  end

  # Helper function to get days in a month
  defp days_in_month(year, month) do
    Date.days_in_month(Date.new!(year, month, 1))
  end
end
