import faust
import numpy as np
import pandas as pd
import pywt
import entropy
import os


app = faust.App('eeg-preprocess', broker='kafka://ip-10-0-0-8.ec2.internal', store='memory://') # DEV

topic = app.topic('chb01', key_type=bytearray, value_type=bytearray) # DEV

# DEV for later -- serialization scheme for key and value
"""
class MyKeyModel(faust.Record):
    channel: str

class MyValueModel(faust.Record):
    time_at_edge: float
    potential: float
"""

# from StackExchange -- generate surrogate series!
def generate_surrogate_series(ts):  # time-series is a Numpy ndarray, or array-like
    ts_fourier  = np.fft.rfft(ts)
    random_phases = np.exp(np.random.uniform(0,np.pi,len(ts)//2+1)*1.0j)
    ts_fourier_new = ts_fourier*random_phases
    new_ts = np.fft.irfft(ts_fourier_new)
    return new_ts


def get_delta_apen(ts): # time-series is a Numpy ndarray, or array-like
    # Perform discrete wavelet transform to get A3, D3, D2, D1 coefficient time series,
    # and create corresponding surrogate time series
    (cA1, cD1) = pywt.dwt(ts, 'db4')
    (cA2, cD2) = pywt.dwt(cA1, 'db4')
    (cA3, cD3) = pywt.dwt(cA2, 'db4')
    coeffs_sample = [cA3, cD3, cD2, cD1]
    coeffs_surrogate = list(map(lambda x: generate_surrogate_series(x), coeffs_sample))

    # Compute the approximate entropy of sample and surrogate coefficients
    app_entropy_sample = list(map(lambda x: entropy.app_entropy(x, order=2, metric='chebyshev'), coeffs_sample))
    app_entropy_surrogate = list(map(lambda x: entropy.app_entropy(x, order=2, metric='chebyshev'), coeffs_surrogate))

    # Return the delta
    delta_ApEns = list(map(lambda x, y: x - y, app_entropy_surrogate, app_entropy_sample))
    return delta_ApEns


@app.agent('eeg-preprocess')
async def process(stream):
    async for event in stream: