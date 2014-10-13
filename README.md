# mortar-luigi

mortar-luigi is an open source collection of extensions for using [Mortar](https://www.mortardata.com/) from within [Luigi](https://github.com/spotify/luigi) data pipelines.

For lots of additional information on mortar-luigi and luigi, see [Mortar's luigi help site](https://help.mortardata.com/technologies/luigi).

[![Build Status](https://travis-ci.org/mortardata/mortar-luigi.png?branch=master)](https://travis-ci.org/mortardata/mortar-luigi)

# Installation

mortar-luigi is installed automatically when using the [Mortar Development Framework](https://help.mortardata.com/technologies/mortar/install_mortar). You can also build and install it from source via:

```bash
git clone git@github.com:mortardata/mortar-luigi.git
cd mortar-luigi
python setup.py egg_info sdist --format=gztar
pip install dist/mortar-luigi-0.1.0.tar.gz
```

# Examples

For a full list of included Luigi Tasks and examples, see the Luigi Tasks section of [Mortar's luigi help site](https://help.mortardata.com/technologies/luigi).
