# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++17 -O3 -fopenmp -Wall
LDFLAGS = -fopenmp

# Name of the final executable
TARGET = verify_cpp_parallel

# List of source (.cpp) and object (.o) files
SOURCES = verify_cpp_parallel.cpp FileUtils.cpp DateTimeUtils.cpp VerificationUtils.cpp
OBJECTS = $(SOURCES:.cpp=.o)

# Default target: build the executable
all: $(TARGET)

# Rule to link the object files into the final executable
$(TARGET): $(OBJECTS)
	$(CXX) $(LDFLAGS) -o $(TARGET) $(OBJECTS)

# Rule to compile a .cpp file into a .o file
%.o: %.cpp *.hpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Target to clean up the build directory
clean:
	rm -f $(OBJECTS) $(TARGET)

.PHONY: all clean