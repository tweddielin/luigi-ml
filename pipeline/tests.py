import luigi
from luigi.contrib.simulate import RunAnywayTarget
import time

class TestHeadTask(luigi.Task):
    id =  luigi.IntParameter()

    def run(self):
        time.sleep(5)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)

    def requires(self):
        for i in range(4):
            yield TestChildTask(id=i)


class TestChildTask(luigi.Task):
    id = luigi.IntParameter()

    def run(self):
        time.sleep(3)
        self.output().done()

    def output(self):
        return RunAnywayTarget(self)
