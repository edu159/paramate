from setuptools import setup

setup(name='paramate',
    version='1.0',
    description='A software to generate parameter studies and handle HPC jobs.',
    url='http://github.com/edu159/paramate',
    author='Eduardo Ramos Fernandez',
    author_email='eduardo.rf159@gmail.com',
    license='Apache2.0',
    packages=['paramate'],
     entry_points={
          'console_scripts': [
              'paramate = paramate.__main__:main'
          ]
    },
    install_requires=[
      'paramiko',
      'pandas',
      'anytree',
      'pyyaml',
      'scp',
      'colorama',
      'progressbar2',
    ],
    include_package_data=True,
    zip_safe=False)
