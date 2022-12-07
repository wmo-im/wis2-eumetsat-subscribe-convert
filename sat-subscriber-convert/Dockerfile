FROM continuumio/miniconda3

# Set working directory for the project
WORKDIR /app
 
# Create Conda environment from the YAML file
COPY environment.yml .
RUN conda env create -f environment.yml

# Activate Conda environment and check if it is working properly
RUN conda activate env
RUN echo "Making sure epct is installed correctly..."
RUN python -c "from epct import api"

# copy file for my project
COPY sat-subscribe-convert.py /tmp/sat-subscribe-convert.py
COPY data_mapping.csv /tmp/data_mapping.csv
	
CMD [ "python3", "-u", "/tmp/sat-subscribe-convert.py" ]