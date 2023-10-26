import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - [%(levelname)s]"
                           " -  %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s",
                    datefmt='%H:%M:%S')

logger = logging.getLogger(__name__)
