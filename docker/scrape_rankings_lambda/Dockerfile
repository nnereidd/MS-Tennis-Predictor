FROM public.ecr.aws/lambda/python:3.11
COPY requirements.txt .
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
COPY rankings_lambda.py ${LAMBDA_TASK_ROOT}/
COPY rankings.py ${LAMBDA_TASK_ROOT}/
COPY functions.py ${LAMBDA_TASK_ROOT}/
CMD ["rankings_lambda.lambda_handler"]
