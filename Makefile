CXX = g++
CXXFLAGS = -std=c++17 -O3 -pthread -Wall

all: master worker

master: master.cpp protocol.hpp
	$(CXX) $(CXXFLAGS) master.cpp -o master

worker: worker.cpp protocol.hpp
	$(CXX) $(CXXFLAGS) worker.cpp -o worker

clean:
	rm -f master worker experiment_results.csv
