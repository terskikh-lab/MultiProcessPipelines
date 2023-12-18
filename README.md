# MultiProcessPipelines -- A Scalable Pipelining Framework for Shared Filesystems in Python

MultiProcessPipelines is a toolkit for creating pipelines, which we will define as a list of processes to be executed in a particular order. MultiProcessPipelines is built on top of MultiProcessTools, developed as a more abstract way of lazily handling shared filesystem parallel computations through tempfile creation and collection.

This project is in its infancy and much of the code is actively being developed (and deleted), so expect major refactoring until future versions specify otherwise. 

MultiProcessPipelines was developed to assist in the computational biology work done in the Terskih Lab at SBP Medical Discovery Institute.