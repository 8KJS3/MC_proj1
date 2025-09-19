all:
	gcc -O3 -o inputgen inputgen.c
	g++ -O3 -o sequential_skiplist driver.cpp

run:
	./sequential_skiplist 1M-allhits.input   

gen:
	./inputgen -n1000000 -h100 -i40 -d20 > 1M-allhits.input
