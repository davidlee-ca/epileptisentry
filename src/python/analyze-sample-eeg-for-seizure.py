import numpy as np
import pandas as pd
import pywt
import entropy
import os


# from StackExchange
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


if __name__ == "__main__":
    sampling_frequency_in_hz = 256
    window_in_seconds = 16
    step_in_seconds = 2

    print("Reading file....")
    df = pd.read_csv("C:\\Users\\wdlee\\dev\\chbmit_csv\\chb01\\chb01_03.csv", header=0)
    print("Done reading file....")

    (num_rows, num_cols) = df.shape
    length_of_signal_in_seconds = num_rows // sampling_frequency_in_hz

    epoch_width = sampling_frequency_in_hz * window_in_seconds
    epoch_step = sampling_frequency_in_hz * step_in_seconds

    # Open a file handle
    analysis_file_name = "C:\\Users\\wdlee\\dev\\results.csv"
    os.makedirs(os.path.dirname(analysis_file_name), exist_ok=True)
    f = open(analysis_file_name, "w+")
    f.write("epoch_start_index,epoch_end_in_sec,channel,raw,A3,D3,D2,D1")

    for channel in list(df.columns):
        for epoch_start_location in range(0, num_rows - epoch_width, epoch_step):
            ds = df[channel].to_numpy()[epoch_start_location:epoch_start_location + epoch_width]

            app_entropy_raw = entropy.app_entropy(ds, order=2, metric='chebyshev')
            app_entropy_raw_surrogate = entropy.app_entropy(generate_surrogate_series(ds), order=2, metric='chebyshev')
            delta_apen_raw = app_entropy_raw_surrogate - app_entropy_raw
            [A3, D3, D2, D1] = get_delta_apen(ds)
            epoch_end_in_sec = epoch_start_location // sampling_frequency_in_hz + window_in_seconds

            print(f"Analyzed window ending at {epoch_end_in_sec}s")
            f.write(f"{epoch_start_location},{epoch_end_in_sec},{channel},{A3:.6f},{D3:.6f},{D2:.6f},{D1:.6f}")

    f.close()
