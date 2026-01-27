const message = @import("message.zig");
const serialization = @import("serialization.zig");
const transport = @import("transport.zig");

pub const Record = message.Record;
pub const MessageType = message.MessageType;
pub const ErrorCode = message.ErrorCode;
pub const AppendRequest = message.AppendRequest;
pub const AppendResponse = message.AppendResponse;
pub const ReadRequest = message.ReadRequest;
pub const ReadResponse = message.ReadResponse;
pub const ErrorResponse = message.ErrorResponse;
pub const ReplicatedEntry = message.ReplicatedEntry;
pub const ReplicateRequest = message.ReplicateRequest;
pub const ReplicateResponse = message.ReplicateResponse;
pub const HeartbeatRequest = message.HeartbeatRequest;
pub const HeartbeatResponse = message.HeartbeatResponse;
pub const Message = message.Message;

pub const ProtocolError = serialization.ProtocolError;
pub const MAX_MESSAGE_SIZE = serialization.MAX_MESSAGE_SIZE;
pub const serializeMessage = serialization.serializeMessage;
pub const deserializeMessage = serialization.deserializeMessage;
pub const deserializeMessageBody = serialization.deserializeMessageBody;

pub const readCompleteMessage = transport.readCompleteMessage;