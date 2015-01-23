package nl.utwente.bigdata.util;

public class Score {
	
	public Score(int s1, int s2) {
		T1goals = s1;
		T2goals = s2;
	}
	
	public int T1goals;
	public int T2goals;
	
	@Override
	public String toString() {
		return "[" + T1goals + " - " + T2goals + "]";
	}
	
    @Override
    public int hashCode() {
        final int prime = 31;
        return prime * T1goals + T2goals; 
    }
	@Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Score other = (Score) obj;
        if (T1goals != other.T1goals || T2goals != other.T2goals)
            return false;
        return true;
    }
}
