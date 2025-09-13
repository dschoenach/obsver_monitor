// VerificationUtils.hpp
#pragma once

#include "DataTypes.hpp"
#include <string>

bool is_missing(double v);
double get_surface_value(const SurfaceStation& s, const std::string& var);
double get_temp_value(const TempLevel& t, const std::string& var);
double directional_diff(double f, double o);