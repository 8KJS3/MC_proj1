all:
	gcc -O3 -pthread -o inputgen inputgen.c
	g++ -O3 -pthread -o sequential_skiplist driver.cpp
	g++ -O3 -o old_skiplist old_driver.cpp

run:
	./sequential_skiplist 1M-allhits.input $(NUM) > new_log4.txt
	./sequential_skiplist huge-allhits.input $(NUM) > new_log3.txt
	./sequential_skiplist tiny-allhits.input $(NUM) > new_log2.txt
	./sequential_skiplist small-allhits.input $(NUM) > new_log1.txt	

gen:
	./inputgen -n1000000 -h100 -i40 -d20 > 1M-allhits.input
