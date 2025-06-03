import grpc
from lms import lms_pb2, lms_pb2_grpc

def main():
    # 1) Connect to the LMS leader
    channel = grpc.insecure_channel("localhost:50051")
    stub    = lms_pb2_grpc.LMSServiceStub(channel)

    # 2) Perform LoginRequest
    login_req = lms_pb2.LoginRequest(
        username="student1",
        password="pass123",
        user_type=lms_pb2.LoginRequest.STUDENT,
    )
    login_resp = stub.Login(login_req, timeout=2)
    token      = login_resp.token

    # 3) Use that token as metadata on PostQuery
    metadata = [("authorization", token)]
    post_req  = lms_pb2.PostQueryRequest(
        title="What is an LLM?",
        body="Explain large language models in simple terms.",
        answer_to_instructor=False,   # False = route to AI Tutor
    )

    try:
        post_resp = stub.PostQuery(post_req, metadata=metadata, timeout=3)
        print("PostQuery succeeded:", post_resp)
    except grpc.RpcError as e:
        print("PostQuery failed:", e.code(), e.details())

if __name__ == "__main__":
    main()
