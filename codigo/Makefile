.PHONY : all clean run new

MPIROOT = /usr/local

CFLAGS+= -Wall -std=c++11 -pthread -g $(INCL)
CC=gcc
MPICC=  $(MPIROOT)/bin/mpic++
INCL= -I$(MPIROOT)/include
SRCS= node.cpp block.cpp blockchain.cpp
BIN= blockchain


all: blockchain

$(BIN): $(SRCS)
	$(MPICC) $(CFLAGS) -o $(BIN) $(SRCS)

clean:
	rm -f $(BIN) *.o

new: clean all

run: blockchain
	mpirun -np 4 --hostfile hostfile ./blockchain