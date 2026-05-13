package com.simulation.analytics;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private Topics topics = new Topics();

    public Topics getTopics() {
        return topics;
    }

    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public static class Topics {
        private String position = "motion-position";
        private String collision = "motion-collision";
        private String analytics = "motion-ball-analytics";

        public String getPosition() {
            return position;
        }

        public void setPosition(String position) {
            this.position = position;
        }

        public String getCollision() {
            return collision;
        }

        public void setCollision(String collision) {
            this.collision = collision;
        }

        public String getAnalytics() {
            return analytics;
        }

        public void setAnalytics(String analytics) {
            this.analytics = analytics;
        }
    }
}
