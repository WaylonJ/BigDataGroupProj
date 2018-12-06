# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 17:18:48 2018

@author: Waylon
"""
import re

#Default hadoop filename
filename='part-00000'

i = 0
holding = []
lineNum = []
find = "ArrDelay"

# This specifically searches through the output of the hadoop system
# and finds the ten most popular routes between any 2 Airports and 
# prints them to console.
while(i < 10):
    largest = 0
    holdLine = 0

    with open(filename) as myFile:
        for num, line in enumerate(myFile, 1):
            compare = int(re.search(r'\d+', line).group())
            
            skip = False
			# Skips the lines that have the delay value printed out, 
			# looking for num of flights.
            if(line.find(find) == -1):
                for item in holding:
                    if(compare == item):
                        skip = True
                if(skip == False):
                    if(compare > largest):
                        largest = compare
                        holdLine = line
            
    holding.append(largest)
    lineNum.append(holdLine)
    
    i = i + 1
    
print(lineNum)