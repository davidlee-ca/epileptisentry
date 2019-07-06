![epileptiSentry Logo](./docs/epileptiSentry-hero-image.jpg)

**epileptiSentry: Real-time electroencephalography (EEG) signal analysis for monitoring seizure activity at scale**

### Table of Contents
1. About
2. Engineering Design
3. Deployment
4. Credits & Contacts

# About
An estimated 3.4 million Americans have epilepsy [1]. Despite its prevalence, accurate diagnosis of epilepsy is difficult: in fact, experts estimate that about 20% of people are misdiagnosed [2]. Electroencephalography (EEG) enables us to visualize brain activity and understand the nature of epileptic seizures, but they require extensive processing [3], and needs to be contextualized with a detailed and reliable account of the corresponding seizure event, which is often unavailable [4]. **_epileptiSentry_** leverages scalable stream processing technologies to:

1. identify potential seizure activities from EEG signals real-time; and 
2. notify the right people to closely monitor the seizure events as they happen

By centralizing the signal processing, **_epileptiSentry_** also helps reduce the costs of closely monitoring patients 1-on-1 for extended periods of time and 

# Engineering Design
[FIXME: in progress]
The processing logic uses a discrete wavelet tranform-based method described by Ocak [5]. 


# Deployment
[FIXME: in progress]

# Credits & Contacts
epileptiSentry was developed by David Lee ([LinkedIn Profile](https://www.linkedin.com/in/wdlee/)). This project was a deliverable from my fellowship in Insight Data Engineering Fellowship program in June 2019 in New York City, NY, United States.

# Reference
[1] U.S. Center for Disease Control and Prevention. [Epilepsy Data and Statistics](https://www.cdc.gov/epilepsy/data/index.html). 
[2] Oto, M. [The misdiagnosis of epilepsy: Appraising risks and managing uncertainty.](https://www.seizure-journal.com/article/S1059-1311(16)30297-7/fulltext) Seizure (2017) **44**:143-6.
[3] Bigdely-Shamlo et al. [The PREP pipeline: standardized preprocessing for large-scale EEG analysis.](https://www.frontiersin.org/articles/10.3389/fninf.2015.00016/full) Front Neuroinform (2015) **9**:16. 
[4] Moeller et al. [Electroencephalography (EEG) in the diagnosis of seizures and epilepsy.](https://www.uptodate.com/contents/electroencephalography-eeg-in-the-diagnosis-of-seizures-and-epilepsy) UpToDate (2018).
[5] Ocak, H. [Automatic detection of epileptic seizures in EEG using discrete wavelet transform and approximate entropy.](http://www.sciencedirect.com/science/article/pii/S0957417407006203) Exp System Appl (2009) **2**, Part 1:2027-2036. 
