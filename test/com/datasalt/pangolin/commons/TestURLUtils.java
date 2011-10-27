package com.datasalt.pangolin.commons;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;


import org.junit.Test;

import com.datasalt.pangolin.commons.URLUtils;
import com.datasalt.pangolin.commons.test.BaseTest;

public class TestURLUtils extends BaseTest{

	@Test
	public void testUrlUtils() throws IOException
	{
		String access_token="access_token";
		String value = "122524694445860|7fc6dd5fe13b43c09dad009d.1-1056745212|An3Xub_HEDRsGxVPkmy71VdkFhQ";
		String url = "https://graph.facebook.com/me/friends?";
		url=url+"access_token";
		url=url+"=";
		url=url+value;
		url=url+"&a";
		url=url+"=1";
		Map m = URLUtils.extractParameters(url);
		System.out.println(m);
		
		//We should see 2 pars in the url : "access_token" and "a" 
		Assert.assertEquals(2, m.size());
		
		//get the acess token value
		String value2 = m.get(access_token).toString();

		//it should be the same as the "value" variable. 
		Assert.assertEquals(value2.length(), value.length());
		Assert.assertEquals(value2, value);
		
	}
}
