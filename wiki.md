---
layout: default
title: wiki test
name: wiki
---

Welcome to the datasalt-utils wiki!

# Titulo1
## Titulo 2
### Titulo 3

[my internal link][Home] 

[enlace](http://www.enlace.com)

![imagen](http://www.datasalt.es/wp-content/themes/redux/datasalt/images/icon-courses.png)

**bold**
_italic_

```java
	public void setConf(Configuration conf) {
		try {
			if(conf != null) {
				this.conf = conf;
				setGrouperConf(TupleMRConfig.get(conf));
				TupleMRConfigBuilder.initializeComparators(conf, this.grouperConf);
				binaryComparator.setConf(conf);
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
```

* Una
* Lista

Pepito

1. Otra
2. Lista
   * Encadenada
   * por que si

> Quote
> text

Horizontal rule:
***


## Markdown plus tables ##

| Header | Header | Right  |
| ------ | ------ | -----: |
|  Cell  |  Cell  |   $10  |
|  Cell  |  Cell  |   $20  |

