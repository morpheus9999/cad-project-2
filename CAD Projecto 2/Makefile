# Define required macros here
SHELL = /bin/sh

all: FileHandler.o
	mpic++ -Wall -o test main.cpp FileHandler.o

# now define how each object file is a target and list dependencies and how
#  to build that object file if any dependencies change

FileHandler.o: FileHandler.cpp FileHandler.h
	mpic++ -c FileHandler.cpp

clean:
	rm *.o

# that's all