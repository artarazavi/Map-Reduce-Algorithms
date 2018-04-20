Question 1
  input: txt file with a number on each line (my test file is test_input.txt)

1a
  To obtain the largest integer run
  $ python get_max_num.py test_input.txt
  the max should = 99

1b
  To obtain the average of all integers run
  $ python get_avg.py test_input.txt
  the average should be = 14.6

1c
  To obtain the same set of integers run
  $ python same_set.py test_input.txt
  the same set should be = [5, 3, 13, 6, 2, 1, 30, 10, 12, 34, 22, 8, 20, 21, 16, 4, 9, 99, 18, 19]

1d
  To count the number of distinct elements in the input run
  $ python count_distinct.py test_input.txt
  the number of distinct elements should be = 20

Question 2
  input is the email file provided by exercise

2a
  To count the number of nodes in the graph run
  $ python count_nodes.py Email-EuAll.txt
  the number of nodes in the graph = 265214

2b
  To get the average and median in and out degree run
  $ python avg_median_degree.py Email-EuAll.txt
  The average in_degree = 1.58
  The median in_degree = 0.0
  The average out_degree = 1.58
  The median out_degree = 0

2c
  To get the average and median number of nodes reachable in 2 hops run
  $ python hop_avg.py Email-EuAll.txt
  The average number of nodes reachable by 2 hops = 174.31
  The median number of nodes reachable by 2 hops = 72

2d
  To get the number of nodes with in_degree > 100 run
  $ in_degree_hundred.py Email-EuAll.txt
  The number of nodes with in_degree > 100 = 702