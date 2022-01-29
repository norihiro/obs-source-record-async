#include <obs-module.h>
#include <obs.h>
#include <util/platform.h>
#include <util/circlebuf.h>
#include <util/threading.h>
#include <util/dstr.h>
#include "media-io/video-frame.h"
#include "plugin-macros.generated.h"

typedef enum async_record_state {
	idle = 0,
	starting,
	running,
	stopping,
} async_record_state;

struct async_record
{
	// properties
	char *directory;
	char *filename_format;
	char *extension;
	obs_data_t *output_data;
	bool overwrite_timestamp;

	// internal data
	obs_source_t *self;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	struct circlebuf video_frames;
	obs_output_t *output;
	video_t *video_output;
	audio_t *audio_output;
	uint64_t last_video_ns;
	uint64_t video_frame_interval;
	// TODO: add audio data
	async_record_state state;
	bool enabled;
	bool need_restart;
	volatile bool record;
	volatile bool close;
	volatile bool failed; // set by thread, reset when data is updated.
	volatile bool output_stopped;

	pthread_t thread;
};

char *make_filename(const char *dir, const char *fmt, const char *ext)
{
	// TODO: Implement the full features of obs-studio's file name generation function.
	struct dstr path;

	dstr_init_copy(&path, dir);

	char *base = os_generate_formatted_filename(ext, false, fmt);
	dstr_cat_ch(&path, '/');
	dstr_cat(&path, base);
	bfree(base);

	return path.array;
}

struct obs_source_frame *peek_first_frame(struct async_record *s)
{
	pthread_mutex_lock(&s->mutex);

	for (;;) {
		blog(LOG_INFO, "%p: waiting first frame", s);
		if (s->close || !s->record || s->failed)
			break;

		if (s->video_frames.size == 0) {
			pthread_cond_wait(&s->cond, &s->mutex);
			continue;
		}

		struct obs_source_frame *frame;
		circlebuf_peek_front(&s->video_frames, &frame, sizeof(frame));

		pthread_mutex_unlock(&s->mutex);

		blog(LOG_INFO, "%p: got first frame: width=%d height=%d", s, frame->width, frame->height);
		return frame;
	}

	pthread_mutex_unlock(&s->mutex);
	return NULL;
}

static bool create_video_output(struct async_record *s)
{
	struct obs_source_frame *frame = peek_first_frame(s);
	if (!frame)
		return false;

	struct obs_video_info ovi = {0};
	obs_get_video_info(&ovi);

	struct video_output_info vi = {0};
	vi.format = frame->format;
	vi.width = frame->width;
	vi.height = frame->height;
	vi.fps_den = ovi.fps_den;
	vi.fps_num = ovi.fps_num;
	vi.cache_size = 16;               // Copied from source-record.c. Why 16?
	vi.colorspace = VIDEO_CS_DEFAULT; // TODO: Can I get colorspace from the source?
	vi.range = frame->full_range ? VIDEO_RANGE_FULL : VIDEO_RANGE_PARTIAL;
	vi.name = obs_source_get_name(s->self);
	if (video_output_open(&s->video_output, &vi) != VIDEO_OUTPUT_SUCCESS)
		return false;

	s->last_video_ns = 0;
	s->video_frame_interval = video_output_get_frame_time(s->video_output);

	return true;
}

void cb_stopped(void *data, calldata_t *cd)
{
	struct async_record *s = data;
	int code = calldata_int(cd, "code");
	if (code != OBS_OUTPUT_SUCCESS) {
		blog(LOG_INFO, "%p: stopped with an error code=%d", s, code);
		s->failed = true;
	}
	s->output_stopped = true;
	pthread_cond_signal(&s->cond);
}

static bool thread_start_loop(struct async_record *s)
{
	pthread_mutex_lock(&s->mutex);

	for (;;) {
		blog(LOG_INFO, "%p: waiting next operation", s);
		s->state = idle;
		if (s->close) {
			pthread_mutex_unlock(&s->mutex);
			return false;
		}

		if (!s->record || s->failed) {
			pthread_cond_wait(&s->cond, &s->mutex);
			continue;
		}

		s->state = starting;

		obs_data_t *data = obs_data_create();
		char *filename = make_filename(s->directory, s->filename_format, s->extension);
		obs_data_set_string(data, "url", filename);
		if (s->output_data)
			obs_data_apply(data, s->output_data);

		s->need_restart = false;

		pthread_mutex_unlock(&s->mutex);

		if (!create_video_output(s)) {
			blog(LOG_ERROR, "%p create_video_output failed", s);
			pthread_mutex_lock(&s->mutex);
			continue;
		}

		// TODO: implement settings
		obs_data_set_int(data, "video_bitrate", 2500);
		obs_data_set_int(data, "audio_bitrate", 320);

		blog(LOG_INFO, "%p: starting filename=%s", s, filename);
		bfree(filename);

		obs_output_t *output = obs_output_create("ffmpeg_output", "async_record", data, NULL);
		obs_data_release(data);
		if (!output) {
			blog(LOG_ERROR, "%p obs_output_create failed", s);
			pthread_mutex_lock(&s->mutex);
			s->failed = true;
			continue;
		}

		s->output_stopped = false;
		signal_handler_t *sh = obs_output_get_signal_handler(output);
		signal_handler_connect(sh, "stop", cb_stopped, s);

		obs_output_set_mixers(output, 1); // TODO: control from properties
		obs_output_set_media(output, s->video_output, obs_get_audio());

		if (!output || !obs_output_start(output)) {
			blog(LOG_ERROR, "%p obs_output_start failed", s);
			obs_output_release(output);
			s->failed = true;
			pthread_mutex_lock(&s->mutex);
			continue;
		}

		s->output = output;
		return true;
	}
}

static void send_video(struct async_record *s, struct obs_source_frame *frame)
{
	if (!s->video_output || video_output_stopped(s->video_output)) {
		blog(LOG_ERROR, "%p: video_output is unavailable", s);
		return;
	}

	const struct video_output_info *info = video_output_get_info(s->video_output);
	if (frame->width != info->width) {
		blog(LOG_INFO, "%p frame width mismatch, got %d, expected %d", s, frame->width, info->width);
	}
	if (frame->height != info->height) {
		blog(LOG_INFO, "%p frame height mismatch, got %d, expected %d", s, frame->height, info->height);
	}
	if (frame->format != info->format) {
		blog(LOG_INFO, "%p frame format mismatch, got %d, expected %d", s, (int)frame->format,
		     (int)info->format);
	}

	int count;
	uint64_t ts = frame->timestamp;
	if (!s->last_video_ns) {
		count = 1;
		s->last_video_ns = ts;
	}
	else {
		count = (int)((ts - s->last_video_ns) / s->video_frame_interval);

		s->last_video_ns += count * s->video_frame_interval;
		ts = s->last_video_ns;

		if (count <= 0) {
			blog(LOG_WARNING, "%p: too many frames received at timestamp=%.3f", s, frame->timestamp * 1e-9);
			return;
		}
	}

	struct video_frame output_frame;
	if (count != 1) {
		blog(LOG_INFO, "%p count=%d frame.timestamp=%.3f ts=%.3f", s, count, frame->timestamp * 1e-9,
		     ts * 1e-9);
	}
	if (!video_output_lock_frame(s->video_output, &output_frame, count, ts)) {
		blog(LOG_ERROR, "%p: video_output_lock_frame failed timestamp=%.3f", s, frame->timestamp * 1e-9);
		return;
	}

	// TODO: implement #planes
	for (int i = 0; i < 1; i++) {
		if (frame->linesize[i] == output_frame.linesize[i]) {
			memcpy(output_frame.data[i], frame->data[i], output_frame.linesize[i] * frame->height);
		}
		else {
			uint8_t *d = output_frame.data[i];
			const uint8_t *s = frame->data[i];
			uint32_t ls = frame->linesize[i];
			if (output_frame.linesize[i] < ls)
				ls = output_frame.linesize[i];
			for (uint32_t y = 0; y < frame->height; y++) {
				memcpy(d, s, ls);
				d += output_frame.linesize[i];
				s += frame->linesize[i];
			}
		}
	}

end:
	video_output_unlock_frame(s->video_output);
}

static void thread_main_loop(struct async_record *s)
{
	pthread_mutex_lock(&s->mutex);
	for (;;) {
		s->state = running;
		if (s->close || !s->record || s->need_restart || s->output_stopped) {
			break;
		}

		if (s->video_frames.size == 0) {
			pthread_cond_wait(&s->cond, &s->mutex);
			continue;
		}

		// TODO: move send_video to the video thread (async_record_video)
		struct obs_source_frame *frame;
		circlebuf_pop_front(&s->video_frames, &frame, sizeof(frame));

		pthread_mutex_unlock(&s->mutex);

		send_video(s, frame);
		obs_source_frame_destroy(frame);

		pthread_mutex_lock(&s->mutex);
	}
	pthread_mutex_unlock(&s->mutex);
}

static void thread_close_loop(struct async_record *s)
{
	blog(LOG_INFO, "%p: closing output", s);
	if (!s->output)
		return;

	blog(LOG_INFO, "%p: stopping", s);
	// obs_output_stop(s->output);
	// TODO: gracely stop and wait

	if (!s->output_stopped) {
		obs_output_force_stop(s->output);
	}

	obs_output_release(s->output);

	if (s->video_output) {
		video_output_close(s->video_output);
		s->video_output = NULL;
	}

	s->output = NULL;
}

static void *async_record_thread(void *data)
{
	os_set_thread_name("asrec");
	struct async_record *s = data;

	while (!s->close) {
		if (!thread_start_loop(s))
			break;

		thread_main_loop(s);

		thread_close_loop(s);
	}

	blog(LOG_INFO, "%p: exiting thread", s);
	return NULL;
}

static const char *async_record_name(void *unused)
{
	UNUSED_PARAMETER(unused);

	return obs_module_text("Asynchronous Source Record");
}

static void free_video_data(struct async_record *s)
{
	// copied from obs-replay-source/replay.c

	while (s->video_frames.size) {
		struct obs_source_frame *frame;

		circlebuf_pop_front(&s->video_frames, &frame, sizeof(frame));

		if (os_atomic_dec_long(&frame->refs) <= 0)
			obs_source_frame_destroy(frame);
	}
}

static obs_properties_t *async_record_get_properties(void *unused)
{
	UNUSED_PARAMETER(unused);
	obs_properties_t *props;
	obs_property_t *prop;

	props = obs_properties_create();

	prop = obs_properties_add_path(props, "directory", obs_module_text("Directory"), OBS_PATH_DIRECTORY, NULL,
				       NULL);
	prop = obs_properties_add_text(props, "filename_format", obs_module_text("Filename format"), OBS_TEXT_DEFAULT);
	prop = obs_properties_add_text(props, "extension", obs_module_text("Extension"), OBS_TEXT_DEFAULT);

	obs_properties_add_bool(props, "overwrite_timestamp",
				obs_module_text("Overwrite video timestamp with OS time"));

	return props;
}

static void async_record_get_defaults(obs_data_t *settings) {}

static void async_record_destroy(void *data)
{
	struct async_record *s = data;

	pthread_mutex_lock(&s->mutex);
	s->close = true;
	pthread_cond_signal(&s->cond);
	pthread_mutex_unlock(&s->mutex);

	pthread_join(s->thread, NULL);

	bfree(s->directory);
	bfree(s->filename_format);
	bfree(s->extension);
	free_video_data(s);

	pthread_cond_destroy(&s->cond);
	pthread_mutex_destroy(&s->mutex);

	bfree(s);
}

static bool get_string(char **dst, obs_data_t *settings, const char *name)
{
	const char *value = obs_data_get_string(settings, name);
	if (!*dst || strcmp(value, *dst)) {
		bfree(*dst);
		*dst = bstrdup(value);
		return true;
	}
	return false;
}

static void async_record_update(void *data, obs_data_t *settings)
{
	struct async_record *s = data;
	bool changed = false;

	pthread_mutex_lock(&s->mutex);

	changed |= get_string(&s->directory, settings, "directory");
	changed |= get_string(&s->filename_format, settings, "filename_format");
	changed |= get_string(&s->extension, settings, "extension");

	s->overwrite_timestamp = obs_data_get_bool(settings, "overwrite_timestamp");

	if (changed) {
		s->failed = false;
		s->need_restart = true;
		pthread_cond_signal(&s->cond);
	}

	pthread_mutex_unlock(&s->mutex);
}

static void on_enable_changed(void *data, calldata_t *cd)
{
	struct async_record *s = data;
	s->enabled = calldata_bool(cd, "enabled");
}

static void *async_record_create(obs_data_t *settings, obs_source_t *source)
{
	struct async_record *s = bzalloc(sizeof(struct async_record));

	pthread_mutex_init(&s->mutex, NULL);
	pthread_cond_init(&s->cond, NULL);

	async_record_update(s, settings);

	pthread_create(&s->thread, NULL, async_record_thread, s);

	signal_handler_t *sh = obs_source_get_signal_handler(source);
	signal_handler_connect(sh, "enable", on_enable_changed, s);
	s->enabled = obs_source_enabled(source);

	return s;
}

static void async_record_tick(void *data, float sec)
{
	struct async_record *s = data;
	UNUSED_PARAMETER(sec);

	if (s->enabled != s->record) {
		pthread_mutex_lock(&s->mutex);
		s->record = s->enabled;
		if (s->enabled)
			free_video_data(s);
		pthread_cond_signal(&s->cond);
		pthread_mutex_unlock(&s->mutex);
	}
}

static struct obs_source_frame *async_record_video(void *data, struct obs_source_frame *frame)
{
	struct async_record *s = data;

	if (s->record && frame->width > 0 && frame->height > 0) {
		struct obs_source_frame *copied_frame =
			obs_source_frame_create(frame->format, frame->width, frame->height);
		obs_source_frame_copy(copied_frame, frame);

		// Not sure this is really required.
		if (s->overwrite_timestamp || !copied_frame->timestamp)
			copied_frame->timestamp = obs_get_video_frame_time();

		pthread_mutex_lock(&s->mutex);
		circlebuf_push_back(&s->video_frames, &copied_frame, sizeof(copied_frame));
		pthread_cond_signal(&s->cond);
		pthread_mutex_unlock(&s->mutex);
	}

	return frame;
}

static void async_record_remove(void *data, obs_source_t *parent)
{
	struct async_record *s = data;

	// TODO: When this function is called? When hiding filter, is this function called?
	blog(LOG_INFO, "async_record_remove(%p)", s);

	pthread_mutex_lock(&s->mutex);

	// TODO: Is it necessary to free video data here?
	s->close = true;
	free_video_data(s);

	pthread_cond_signal(&s->cond);
	pthread_mutex_unlock(&s->mutex);
}

const struct obs_source_info async_record_info = {
	.id = "net.nagater.obs-async_record",
	.type = OBS_SOURCE_TYPE_FILTER,
	.output_flags = OBS_SOURCE_VIDEO | OBS_SOURCE_ASYNC,
	.get_name = async_record_name,
	.create = async_record_create,
	.destroy = async_record_destroy,
	.update = async_record_update,
	.get_properties = async_record_get_properties,
	.get_defaults = async_record_get_defaults,
	.filter_video = async_record_video,
	.video_tick = async_record_tick,
	.filter_remove = async_record_remove,
};
