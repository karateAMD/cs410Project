###Algorithm to calculate Mean Rating, Number of Reviews, and Mode Rating

map input: [productID, rating]

map output: productID -> mean(=rating), numReviews=1, (a,b,c,d,e)

reduce computation:
	1. sum = mean* numReviews + mean* numReviews + ...
	2. mean = sum / totalNumReviews
	3. acc_tuple += tuple (for the mode)

reduce output: productID -> mean, numReviews, acc_tuple

map phase 2 output: prettify(productID, mean, numReviews, max(acc_tuple))