package com.jcalc.feed;

public class TokenGenerator {
	private String[] tokens;
	private int[] cumProbs;		// elements have range between 0 and 100. expression percentages
	
	public TokenGenerator(String[] tokens, int[] probs){
		assert tokens.length == probs.length:"Must have equal number of Tokens and Probs";
		this.tokens = tokens;
		this.cumProbs = new int[probs.length];
		int cum = 0;
		int i = 0;
		for (int prob : probs){
			cum += prob;
			this.cumProbs[i++] = cum;
		}
		
	}

	public String nextToken(){
		int randIndex = (int)(100 * Math.random());
		int index = 0;
		for (int prob : cumProbs){
			if (randIndex <= prob){
				return tokens[index];
			}
			index++;
		}
		assert false: "should not reach here";
		return "No Token";
	}
	
}

