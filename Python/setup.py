from distutils.core import setup

setup(
    name='timbermill',
    packages=['timbermill'],
    version='0.1',
    license='APACHE',
    description='A Task-Based, Context-Aware Logging service',
    author='Shimon Klebanov',
    author_email='shimonkle@gmail.com',
    url='https://github.com/datorama/Timbermill',  # Provide either the link to your github or to your website
    download_url='https://github.com/user/reponame/archive/v_01.tar.gz',  # TODO: need to change
    keywords=['Logging', 'task-based'],
    install_requires=[
        'certifi',
        'chardet',
        'idna',
        'requests',
        'urllib3',
    ],
    classifiers=[
        'Development Status :: 4 - Alpha',  # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'LICENSE :: OSI APPROVED :: APACHE SOFTWARE LICENSE',
        'Programming Language :: Python :: 3.7',
    ],
)
