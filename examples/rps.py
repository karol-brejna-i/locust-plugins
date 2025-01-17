from locust_plugins.tasksets import TaskSetRPS
from locust import HttpLocust, task


class UserBehavior(TaskSetRPS):
    @task
    def my_task(self):
        self.rps_sleep(2)
        self.client.post("/authentication/1.0/getResults", {"username": "something"})


class MyHttpLocust(HttpLocust):
    task_set = UserBehavior
    wait_time = constant(0)
