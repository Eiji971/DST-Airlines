FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app


RUN pip install --no-cache-dir uvicorn==0.22.0 fastapi==0.100.0 joblib==1.2.0 numpy==1.25.0 pydantic==2.0.2 pandas==1.5.3 scikit-learn==1.2.2 python-multipart
# Copy the FastAPI application code to the container
COPY api/api_model_authentification.py ./api/

COPY preprocessing_fucntion/data_preprocessor_fct.py ./preprocessing_fucntion/

COPY models/gradient_boost_model.pkl ./models/
COPY models/random_forest_model.pkl ./models/

# Expose the FastAPI port (default is 8000)
EXPOSE 8000

# Command to run the FastAPI application
CMD ["uvicorn", "api.api_model_authentification:api", "--host", "0.0.0.0", "--port", "8000"]