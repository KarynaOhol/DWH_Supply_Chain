
SET ROLE dwh_cleansing_user;
SET search_path = BL_CL, BL_3NF, BL_DM, public;


-- Function to determine if a date is a  Holiday
CREATE OR REPLACE FUNCTION BL_CL.is_holiday(check_date DATE, OUT is_holiday BOOLEAN, OUT holiday_name VARCHAR(100))
AS
$$
DECLARE
    year_val      INTEGER;
    month_val     INTEGER;
    day_val       INTEGER;
    day_of_week   INTEGER;
    easter_date   DATE;
    good_friday   DATE;
    easter_monday DATE;
    --utils variables to calculate Easter date
    a             INTEGER;
    b             INTEGER;
    c             INTEGER;
    d             INTEGER;
    e             INTEGER;
    f             INTEGER;
    g             INTEGER;
    h             INTEGER;
    i             INTEGER;
    k             INTEGER;
    l             INTEGER;
    m             INTEGER;
    n             INTEGER;
    p             INTEGER;

BEGIN
    year_val := EXTRACT(YEAR FROM check_date);
    month_val := EXTRACT(MONTH FROM check_date);
    day_val := EXTRACT(DAY FROM check_date);
    day_of_week := EXTRACT(DOW FROM check_date); -- 0=Sunday, 6=Saturday

    is_holiday := FALSE;
    holiday_name := NULL;

    -- Calculate Easter date using the algorithm for Western Christianity
    a := year_val % 19;
    b := year_val / 100;
    c := year_val % 100;
    d := b / 4;
    e := b % 4;
    f := (b + 8) / 25;
    g := (b - f + 1) / 3;
    h := (19 * a + b - d - g + 15) % 30;
    i := c / 4;
    k := c % 4;
    l := (32 + 2 * e + 2 * i - h - k) % 7;
    m := (a + 11 * h + 22 * l) / 451;
    n := (h + l - 7 * m + 114) / 31;
    p := (h + l - 7 * m + 114) % 31;

    easter_date := DATE(year_val || '-' || n || '-' || (p + 1));

    -- Calculate derived Easter-based holidays
    good_friday := easter_date - INTERVAL '2 days';
    easter_monday := easter_date + INTERVAL '1 day';


    -- New Year's Day
    IF month_val = 1 AND day_val = 1 THEN
        is_holiday := TRUE;
        holiday_name := 'New Year''s Day';
        -- Epiphany (January 6)
    ELSIF month_val = 1 AND day_val = 6 THEN
        is_holiday := TRUE;
        holiday_name := 'Epiphany';
        -- Good Friday
    ELSIF check_date = good_friday THEN
        is_holiday := TRUE;
        holiday_name := 'Good Friday';
        -- Easter Sunday
    ELSIF check_date = easter_date THEN
        is_holiday := TRUE;
        holiday_name := 'Easter Sunday';
        -- Easter Monday
    ELSIF check_date = easter_monday THEN
        is_holiday := TRUE;
        holiday_name := 'Easter Monday';
        -- Labour Day (May 1)
    ELSIF month_val = 5 AND day_val = 1 THEN
        is_holiday := TRUE;
        holiday_name := 'Labour Day';
        -- Europe Day (May 9)
    ELSIF month_val = 5 AND day_val = 9 THEN
        is_holiday := TRUE;
        holiday_name := 'Europe Day';
        -- All Saints' Day (November 1)
    ELSIF month_val = 11 AND day_val = 1 THEN
        is_holiday := TRUE;
        -- Christmas Day
    ELSIF month_val = 12 AND day_val = 25 THEN
        is_holiday := TRUE;
        holiday_name := 'Christmas Day';
    END IF;
END;
$$ LANGUAGE plpgsql;