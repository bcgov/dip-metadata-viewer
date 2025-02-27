[![img](https://img.shields.io/badge/Lifecycle-Stable-97ca00)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-stable.md)[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


## dip-metadata-viewer

The DIP Metadata Viewer is a web-based, interactive table for viewing and filtering the data dictionaries for data sets available through the [Data Innovation Program (DIP)](https://www2.gov.bc.ca/gov/content/data/about-data-management/data-innovation-program)&mdash;a data integration and analytics program for the Government of British Columbia. 

The dip-metadata-viewer is built using the [{flexdashboard}](https://rmarkdown.rstudio.com/flexdashboard/), [{crosstalk}](https://rstudio.github.io/crosstalk/index.html) and [{reactable}](https://glin.github.io/reactable/) R packages. All metadata is sourced from the [B.C. Data Catalogue](https://catalogue.data.gov.bc.ca/group/data-innovation-program) using the [{bcdata}](https://bcgov.github.io/bcdata/) R package. All the data described in the dip-metadata-viewer is licensed [Access Only](https://www2.gov.bc.ca/gov/content?id=1AAACC9C65754E4D89A118B875E0FBDA) and is only available through the [Data Innovation Program](https://www2.gov.bc.ca/gov/content/data/about-data-management/data-innovation-program)&mdash;the data described in the dip-metadata-viewer is _not_ downloadable directly from the B.C. Data Catalogue.

### Code
There are two scripts that are required to generate the viewer, they need to be run in sequence: (1) the `get-data.R` script to get and tidy the metadata from the B.C. Data Catalogue using the `bcdata` package and (2) the `dip-metadata-viewer.Rmd` R Markdown file to generate or "knit" the html dip-metadata-viewer, which is deployed using [GitHub pages](https://bcgov.github.io/dip-metadata-viewer/dip-metadata-viewer.html).

All of the required packages can be installed from CRAN using `install.packages()`.

### Getting Help or Reporting an Issue

To report bugs/issues/feature requests, please file an [issue](https://github.com/bcgov/dip-metadata-viewer/issues/).


### How to Contribute

If you would like to contribute, please see our [CONTRIBUTING](CONTRIBUTING.md) guidelines.

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

### License

```
Copyright 2021 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
```
---
*This project was created using the [bcgovr](https://github.com/bcgov/bcgovr) package.* 
