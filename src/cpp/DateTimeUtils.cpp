// DateTimeUtils.cpp (Corrected)
#include "DateTimeUtils.hpp"

// Helper to check for a leap year.
static inline bool is_leap(int y) {
    return ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0));
}

// Helper to get the number of days in a given month and year.
static inline int days_in_month(int y, int m) {
    static const int md[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (m == 2) return md[1] + (is_leap(y) ? 1 : 0);
    return md[m - 1];
}

// This is the only function that should be in this file.
long long add_hours_to_yyyymmddhh(long long start_time, int hours_to_add) {
    int year = (int)(start_time / 1000000LL);
    int month = (int)((start_time / 10000LL) % 100LL);
    int day = (int)((start_time / 100LL) % 100LL);
    int hour = (int)(start_time % 100LL);

    long long total_hours = (long long)hour + hours_to_add;

    while (total_hours >= 24) {
        total_hours -= 24;
        day++;
        int dim = days_in_month(year, month);
        if (day > dim) {
            day = 1;
            month++;
            if (month > 12) { month = 1; year++; }
        }
    }
    while (total_hours < 0) {
        total_hours += 24;
        day--;
        if (day < 1) {
            month--;
            if (month < 1) { month = 12; year--; }
            day = days_in_month(year, month);
        }
    }
    return (long long)year * 1000000LL + (long long)month * 10000LL + (long long)day * 100LL + total_hours;
}