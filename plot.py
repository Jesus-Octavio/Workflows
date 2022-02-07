#!/usr/bin/python

import sys
import pandas as pd
import matplotlib.pyplot as plt

print ('Number of arguments: %i arguments' % len(sys.argv))
print ('Argument List:' + str(sys.argv))

data = pd.read_csv(sys.argv[1],delimiter=';')
data['date'] = pd.to_datetime(data['date'])
data.plot(x='date', y='Temp')
plt.show()