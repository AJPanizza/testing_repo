from org_logging import bind_context, get_logger


def test_org_logging():

    bind_context(project="demo", env="test", run_id="123")
    logger = get_logger()
    logger.info("org_logging imported successfully")
