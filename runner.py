from usgpo.extractor import Extractor
import os


if __name__ == "__main__":

    this_dir = os.path.split(os.path.realpath(__file__))[0]
    config   = os.path.join(this_dir, 'config.json')

    Extractor(config).run_solution()
