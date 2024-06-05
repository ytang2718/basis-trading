import time
import logging
import threading
import numpy as np
from typing import Callable, Optional, Union
from lib.common.numbers import trim_sig_figs
from collections import deque

logger = logging.getLogger(__name__)

class MetricCollector:
    def __init__(self, name: str, metric_polling_function: Callable[[], Optional[Union[int, float]]], polling_interval=1, max_length=600):
        self.name = name
        self.metric_polling_function = metric_polling_function
        self.polling_interval = polling_interval  
        self.data = deque(maxlen=max_length)
        self.last_log_timestamp = 0
        self.start()

    def start(self):
        logger.info(f"{self.name} Metric Collector initilized with polling interval: {self.polling_interval}, max length: {self.data.maxlen}")
        thread = threading.Thread(target=self._run)
        thread.name = f"{self.name} Metric Collector"
        thread.daemon = True
        thread.start()

    def shutdown(self):
        pass

    def _run(self):
        while True:
            data_point = self.metric_polling_function()
            if data_point:
                self.data.append(data_point)
                if time.time() - self.last_log_timestamp >= 10:
                    self.last_log_timestamp = time.time()
                    logger.info(f"{self.name} Metric Collector: gathered new data point: {trim_sig_figs(data_point, 6)}, data cache length: {len(self.data)}")
            time.sleep(self.polling_interval)

    def get_data(self):
        return np.array(self.data)
