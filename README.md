# IoT-23 Analysis

This repository contains a analysis of supervised and unsupervised machhine learning and deep learning techniques on the IoT-23 Dataset. 

## Introduction

Internet of Things (IoT) has drastically increased in relevance in recent years. As our world becomes more interconnected by these low power devices, there is a pressing need to adopt security measures for IoT. These devices are characterized by their unique thin profile, often lacking system security capabilities making securing these on the network level one of the only countermeasures available. 

One of the available techniques for Network Security is Network Intrusion Detection Systems (NIDS). NIDS works by analyzing network artifacts and determining if an intrusion has occurred. This notably happens out of line and does not prevent traffic, which is namely done by Network Intrusion Prevention Systems (NIPS). 

A large body of research is available exploring AI and Machine Learning algorithms / techniques for NIDS. Supervised techniques can be trained on flows that have been labeled to a specific malware class while unsupervised techniques focus on anomaly detection, relying on the necessary assumption that malicious flows are a subset of anomalous ones. 

Unsupervised techniques are superior for detection of zero-day attacks as supervised techniques require samples of the malicious flows, which is not possible in this specific case. For this reason, these techniques show promise for use in IoT and are explored further in this project.

## Dataset

The dataset used is IoT-23, which is a labeled dataset of benign and malicious real IoT traffic. These flows were captured using Zeek (commonly referred to as its old name Bro). Zeek differs from traditional Netflow or IPFIX in that is captures deeper information about the connection. 

The dataset can be found here:

https://www.stratosphereips.org/datasets-iot23

