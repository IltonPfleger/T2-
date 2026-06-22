CXX           := g++
HERE          := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
BUILD         := $(HERE)/build
TARGET        := $(BUILD)/main

SRC           := $(shell find $(HERE)/src -name "*.cpp")
OBJ           := $(patsubst $(HERE)/src/%.cpp,$(BUILD)/%.o,$(SRC))
DEPENDENCIES  := $(OBJ:.o=.d)

CXXFLAGS      := -Iinclude -MMD -MP -std=c++23

all: $(TARGET)
	$(TARGET)

$(TARGET): $(OBJ)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) $^ -o $@

$(BUILD)/%.o: $(HERE)/src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD) logs seqs

-include $(DEPENDENCIES)

.PHONY: clean
