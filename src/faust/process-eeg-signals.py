import faust


class EegKey(faust.Record, serialize='json'):
    ptid: str
    ch: str


class EegReading(faust.Record, serializer='json'):
    time: float
    potential: float


app = faust.App('process-eeg', broker='kafka://ip-10-0-0-4.ec2.internal:9092')
source_topic = app.topic('eeg-signals', key_type=EegKey, value_type=EegReading)
reading_table = app.Table()


# from StackExchange -- generate surrogate series!
def generate_surrogate_series(ts):  # time-series is an array
    ts_fourier  = np.fft.rfft(ts)
    random_phases = np.exp(np.random.uniform(0,np.pi,len(ts)//2+1)*1.0j)
    ts_fourier_new = ts_fourier*random_phases
    new_ts = np.fft.irfft(ts_fourier_new)
    return new_ts.tolist()


def get_delta_apen(ts): # time-series is an array
    # Perform discrete wavelet transform to get A3, D3, D2, D1 coefficient time series,
    # and create corresponding surrogate time series
    (cA1, cD1) = pywt.dwt(ts, 'db4')
    (cA2, cD2) = pywt.dwt(cA1, 'db4')
    cD2_surrogate = generate_surrogate_series(cD2)
    app_entropy_sample = entropy.app_entropy(cD2, order=2, metric='chebyshev')
    app_entropy_surrogate = entropy.app_entropy(cD2_surrogate, order=2, metric='chebyshev')

    # Return the delta
    delta_ApEns = app_entropy_surrogate - app_entropy_sample
    return delta_ApEns


@app.task()
async def determine_seizure(stream):
    async for values in stream.take(4096, within=16):

        time_series = [(x.time, x.potential) for x in values]
        time_series = sorted([time_series], key=lambda x: x[0]) # sort the time-series by instrument timestamp
        delta_apen = get_delta_apen(time_series)

        print(f'RECEIVED {len(values)} samples with a delta_apen of {delta_apen}')


async def create_hopping_windows()


@app.agent(source_topic )
async def count_readings(readings):
    async for pt_waveset in readings.group_by(EegKey.ptid):




@app.timer(2.0)
async def start_rolling_windows():


















if __name__ == '__main__':
    app.main()
