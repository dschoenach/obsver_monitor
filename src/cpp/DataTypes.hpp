// DataTypes.hpp
#pragma once

#include <string>
#include <vector>
#include <unordered_map>

struct SurfaceStation {
    int id = -1;
    double lat = -999.0, lon = -999.0, hgt = -999.0;
    double nn = -999.0, dd = -999.0, ff = -999.0, tt = -999.0, rh = -999.0, ps = -999.0;
    double pe = -999.0, qq = -999.0, vi = -999.0, td = -999.0, tx = -999.0, tn = -999.0;
    double gg = -999.0, gx = -999.0, fx = -999.0;
};

struct TempLevel {
    int station_id = -1;
    double pressure = -999.0;
    double temp = -999.0;
    double fi   = -999.0;
    double rh   = -999.0;
    double qq   = -999.0;
    double dd   = -999.0;
    double ff   = -999.0;
};

struct VobsData {
    std::unordered_map<int, SurfaceStation> stations;
    std::vector<TempLevel> temp_levels;
};

struct FileInfo {
    std::string path, type, experiment;
    long long base_time = 0, valid_time = 0;
    int lead_time = -1;
};

struct ResultKey {
    std::string experiment;
    int lead_time;
    std::string variable;
    long long vt_hour;

    bool operator<(const ResultKey& other) const {
        if (experiment != other.experiment) return experiment < other.experiment;
        if (lead_time != other.lead_time) return lead_time < other.lead_time;
        if (vt_hour != other.vt_hour) return vt_hour < other.vt_hour;
        return variable < other.variable;
    }
};

struct TempResultKey {
    std::string experiment;
    int lead_time;
    std::string variable;
    double pressure_level;
    long long vt_hour;

    bool operator<(const TempResultKey& other) const {
        if (experiment != other.experiment) return experiment < other.experiment;
        if (lead_time != other.lead_time) return lead_time < other.lead_time;
        if (vt_hour != other.vt_hour) return vt_hour < other.vt_hour;
        if (pressure_level != other.pressure_level) return pressure_level < other.pressure_level;
        return variable < other.variable;
    }
};

struct AggregatedStats {
    double sum_of_errors = 0.0, sum_of_squared_errors = 0.0;
    long count = 0;
};