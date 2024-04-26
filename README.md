# Airflow Dremio Provider

This is the official provider package for airflow that integrates with Dremio. It contains the following operators and sensors
* `DremioCreateReflectionOperator` - Operator to manage reflections in Dremio
* `DremioCreateSourceOperator` - Operator to create a source in Airflow
* `DremioJobSensor` - Sensor to check the status of a job submitted in dremio

## Installation
 You can use `pip` to install this package
```commandline
pip install airflow-dremio-provider
```

If you want to build from source, you can do so using [hatch](https://hatch.pypa.io/). First create the .whl file usin the command  `hatch build`. This will create a wheel file in the `dist` directory. To install the package run `pip install *path-to-.whl-file*`

## Creating a Dremio Connection
A dremio connection is the same as an Http connection. Therefor you can just create a connection with type being `HTTP`. Once you have created a connection enter all the fields as required. The login field must contain your username For providing the authentication there are two supported ways

#### Authentication using basic auth
This simply requires authentication to be done using the username and password. The Dremio hook automatically uses this to create an auth token from Dremio and uses this token for all REST API authentication. To use this method of authentication, the `extra` field of the connection add the following - `{"auth":"AuthToken"}`.

#### Authentication using PAT token
This method of authentication is to be done using the user generated PAT token in Dremio. The Dremio hook uses this token for all REST API authentication. To use this method of authentication, the `extra` field of the connection add the following - `{"auth":"PAT", "pat":"your-pat-token-value"}`.


# Contributing
We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to this provider package

# License
This project is licensed under the [Apache 2.0 License](LICENSE).
