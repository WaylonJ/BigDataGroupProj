# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 19:04:14 2018

@author: Waylon
"""

import re


filename='part-00001'

i = 0
holding = []
lineNum = []
find = "ArrDelay"

# This specifically searches through the output of the hadoop system
# and finds the ten most popular TailNumber and prints it to console.
while(i < 10):
    largest = 0
    holdLine = 0

    with open(filename) as myFile:
        for num, line in enumerate(myFile, 1):
            if(line.find(find) == -1):
				compare = line.split()
                
                skip = False
                for item in holding:
                    if(int(compare[1]) == item):
                        skip = True
                if(skip == False):
                    if(int(compare[1]) > largest):
                        largest = int(compare[1])
                        holdLine = line
            
    holding.append(largest)
    lineNum.append(holdLine)
    
    i = i + 1
    
print(lineNum)
