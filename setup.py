from setuptools import setup, find_packages

setup(name="NCAR Flight Monitor",
      version='0.02',
      description='Monitor in flight data for RAF field projects in real time.',
      author='Ryan Orendorff',
      author_email='ryan@rdodesigns.com',
      url='http://github.com/rdodesigns/NCARFlightMonitor',
      packages=find_packages(),
      install_requires=[
        'psycopg2>=2.4.2',
        'Twisted>=11.0.0'
        ]
)
