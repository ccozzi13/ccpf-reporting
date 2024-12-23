from prefect import flow, task

@task(log_prints=True)
def say_hello(name: str):
    print(f"Hello, {name}!")

@flow
def hello_universe(names: list[str]):
    for name in names:
        say_hello(name)

if __name__ == "__main__":
#    hello_universe.deploy(name="hello_universe",
#        work_pool_name="TestPool",
#        tags=["onboarding"],
#        parameters={"names": ['Marvin', 'Trillian', 'Ford']}
#    )
    flow.from_source(
        source="https://github.com/ccozzi13/ccpf-reporting.git", 
        entrypoint="testing/hello_world.py:hello_universe"
    ).deploy(
        name="my-first-deployment", 
        work_pool_name="TestPool", 
    )