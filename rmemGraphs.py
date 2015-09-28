#!/usr/bin/python

# import subprocess
import numpy as np
# import scipy.stats
import matplotlib
matplotlib.use('Agg')
# import itertools
from matplotlib import pyplot as plt

# import pickle
# import pdb


def numFlowsGraph():
    data = '''
    5117725
    4957775
    3925335
    1898360
    1564935
    1101315
    624880
    169210
    25'''
    data = [int(d) for d in data.split('\n') if d != ''][::-1]
    keys_sorted = ['10.0', '20.0', '30.0', '40.0', '50.0', '60.0', '70.0', '80.0', '90.0']
    ind = np.arange(len(keys_sorted))

    plt.cla()
    plt.clf()
    width = 0.35
    plt.xlabel('Percentage of Remote Memory')
    plt.xticks(ind + (width), keys_sorted)
    plt.ylabel('Number of Memory Flows')
    plt.ylim(0, 6e6)
    for i in (1e6 * np.array(range(6, 15))):
        plt.axhline(i, color='lightgray')

    plt.bar(ind, data, color='lightgreen')
    plt.savefig('rmem_numflows.pdf')


def trafficVolumeGraph():
    data = '''
    196577832960
    284444364800
    247280803840
    120661401600
    93570027520
    72290734080
    42847395840
    11872624640
    1679360'''
    data = [float(d)/1e9 for d in data.split('\n') if d != ''][::-1]
    keys_sorted = ['10.0', '20.0', '30.0', '40.0', '50.0', '60.0', '70.0', '80.0', '90.0']
    ind = np.arange(len(keys_sorted))

    plt.cla()
    plt.clf()
    width = 0.35
    plt.xlabel('Percentage of Remote Memory')
    plt.xticks(ind + (width), keys_sorted)
    plt.ylabel('Memory Traffic Volume, GB')
    plt.ylim(0, 300)
    for i in [25*j for j in xrange(1, 12)]:
        plt.axhline(i, color='lightgray')
    plt.bar(ind, data, color='lightgreen')
    plt.savefig('rmem_trafficvolume.pdf')

if __name__ == '__main__':
    numFlowsGraph()
    trafficVolumeGraph()
