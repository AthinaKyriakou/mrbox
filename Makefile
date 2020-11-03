SHELL := /bin/bash

PROJECT_NAME=test10
MAIN_EXEC=./core/mrbox.py
PATH_FOLDER=/home/athina/Desktop/praktiki/mrbox
HDFS_HOST=192.168.1.122
HADOOP_PATH=/home/athina/hadoop-3.2.1

help:
	@echo "Help"

setup:
	wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
	bash Miniconda3-latest-Linux-x86_64.sh

build:
	source ~/miniconda3/etc/profile.d/conda.sh && \
	conda create --name $(PROJECT_NAME) --file requirements.txt

run:
	source ~/miniconda3/etc/profile.d/conda.sh && \
	conda activate $(PROJECT_NAME) && \
	export PYTHONPATH=$(PATH_FOLDER):${PYTHONPATH} && \
	cd src && \
	python $(MAIN_EXEC)

.PHONY: help build run