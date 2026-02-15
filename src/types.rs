pub fn decimal_to_string(value: i128, scale: u8) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let abs = value.unsigned_abs();
    let sign = if value < 0 { "-" } else { "" };
    let divisor = 10u128.pow(scale as u32);
    let integer = abs / divisor;
    let fraction = abs % divisor;
    format!(
        "{sign}{integer}.{fraction:0>width$}",
        width = scale as usize
    )
}

pub fn unix_days_to_iso(unix_days: i32) -> String {
    let days = unix_days + 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

pub fn nanos_to_time_str(nanos: u64) -> String {
    let total_secs = nanos / 1_000_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    let frac = nanos % 1_000_000_000;
    if frac == 0 {
        format!("{:02}:{:02}:{:02}", h, m, s)
    } else {
        let frac7 = frac / 100; // 7 digits
        format!("{:02}:{:02}:{:02}.{:07}", h, m, s, frac7)
    }
}

pub fn micros_to_iso(micros: i64) -> String {
    let total_secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000) as u64;
    let days = total_secs.div_euclid(86400) as i32;
    let day_secs = total_secs.rem_euclid(86400) as u64;
    let date = unix_days_to_iso(days);
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    if frac == 0 {
        format!("{}T{:02}:{:02}:{:02}", date, h, m, s)
    } else {
        format!("{}T{:02}:{:02}:{:02}.{:06}", date, h, m, s, frac)
    }
}

pub fn micros_offset_to_iso(micros: i64, offset_minutes: i16) -> String {
    let base = micros_to_iso(micros);
    if offset_minutes == 0 {
        format!("{}Z", base)
    } else {
        let sign = if offset_minutes >= 0 { '+' } else { '-' };
        let abs = offset_minutes.unsigned_abs();
        format!("{}{}{:02}:{:02}", base, sign, abs / 60, abs % 60)
    }
}
