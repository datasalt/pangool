package com.datasalt.pangolin.commons;

import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Utils for general URL tasks.
 * @author jaylinux
 *
 */
public class URLUtils {
	static Logger log = Logger.getLogger(URLUtils.class);
	public static String getBase(String url)
	{
		return StringUtils.substringBefore(url,"?");
	}
	/**
	 * build a url from a hashtable and a base.  Not tested but may be useful for building urls from 
	 * other data.  Also might be usefull for testing later on .
	 * @param base
	 * @param pars
	 * @return
	 */
	@SuppressWarnings("rawtypes")
  public static String getUrlFromParameters(String base, Map pars)
	{
		StringBuffer s = new StringBuffer(base);
		if(pars.size()>0)
			s.append("?");
		for(Object k : pars.keySet())
		{
			s.append(k);
			s.append("=");
			s.append(pars.get(k));
		}
		return s.toString();
	}
	/**
	   Extract parameters from a url so that Ning (or other crawling utilities) 
	   can properly encode them if necessary. This was necesssary for facebook requests, 
	   which are bundled with "next urls" that have funny characters in them such as this 
	   
	   "122524694445860|7fc6dd5fe13b43c09dad009d.1-1056745212|An3Xub_HEDRsGxVPkmy71VdkFhQ"
	 * @param url
	 * @return
	 */
	public static Hashtable<String,String> extractParameters(String url)
	{
		Hashtable<String,String> pars = new Hashtable<String,String>();
		if(! url.contains("?"))
		{
			log.warn("WARNING : URL HAS NO PARAMETERS ! " + url);
			return pars;
		}
		String parameters = StringUtils.substringAfter(url,"?");
		for(String pairs : parameters.split("&"))
		{	
			String[] nv=pairs.split("=");
			pars.put(nv[0],nv[1]);
		}
		return pars;
	}

}
