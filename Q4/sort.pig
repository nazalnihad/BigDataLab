-- number_sort.pig
-- Load the numbers
numbers = LOAD '/user/q4/input/numbers.txt' AS (value:int);

-- Sort the numbers
sorted_numbers = ORDER numbers BY value ASC;

-- Store the sorted result
STORE sorted_numbers INTO '/user/q4/output/sorted_numbers';